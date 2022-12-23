use std::{fs::File, path::Path, time::Duration};

use anyhow::{Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use md4::{Digest, Md4};
use memmap::Mmap;
use ranidb::AniDb;
use rayon::{iter::ParallelIterator, slice::ParallelSlice};
use rusqlite::params;
use tokio::fs;

const ED2K_CHUNK_SIZE: usize = 9728000;

fn ed2k_hash(file: &File, pb: &ProgressBar) -> std::io::Result<[u8; 16]> {
    let map = unsafe { Mmap::map(file) }?;

    pb.set_length(map.len() as u64 / ED2K_CHUNK_SIZE as u64);

    let hashes: Vec<[u8; 16]> = map
        .par_chunks(ED2K_CHUNK_SIZE)
        .map(Md4::digest)
        .map(Into::into)
        .inspect(|_| pb.inc(1))
        .collect();

    let root_hash = Md4::digest(hashes.concat());

    Ok(root_hash.into())
}

fn init_database(db_path: &Path) -> Result<rusqlite::Connection> {
    let conn = rusqlite::Connection::open(db_path).context("failed to open db")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS anime (
            aid                 INTEGER PRIMARY KEY,
            dateflags           INTEGER,
            year                TEXT,
            atype               TEXT,
            related_aid_list    TEXT,
            related_aid_type    TEXT,
            romaji_name         TEXT,
            kanji_name          TEXT,
            english_name        TEXT,
            short_name_list     TEXT,
            episodes            INTEGER,
            special_ep_count    INTEGER,
            air_date            INTEGER,
            end_date            INTEGER,
            picname             TEXT,
            nsfw                BOOLEAN,
            characterid_list    TEXT,
            specials_count      INTEGER,
            credits_count       INTEGER,
            other_count         INTEGER,
            trailer_count       INTEGER,
            parody_count        INTEGER
        )",
        params![],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS episodes (
            eid                 INTEGER PRIMARY KEY,
            aid                 INTEGER,
            length              INTEGER,
            rating              INTEGER,
            votes               INTEGER,
            epno                TEXT,
            eng                 TEXT,
            romaji              TEXT,
            kanji               TEXT,
            aired               INTEGER,
            etype               INTEGER,

            FOREIGN KEY (aid) REFERENCES anime (aid)
        )",
        params![],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            fid                 INTEGER PRIMARY KEY,
            aid                 INTEGER,
            eid                 INTEGER,
            gid                 INTEGER,
            state               INTEGER,
            size                INTEGER,
            ed2k                TEXT,
            colour_depth        TEXT,
            quality             TEXT,
            source              TEXT,
            audio_codec_list    TEXT,
            audio_bitrate_list  INTEGER,
            video_codec         TEXT,
            video_bitrate       INTEGER,
            video_resolution    TEXT,
            dub_language        TEXT,
            sub_language        TEXT,
            length_in_seconds   INTEGER,
            description         TEXT,
            aired_date          INTEGER,

            FOREIGN KEY (aid) REFERENCES anime (aid),
            FOREIGN KEY (eid) REFERENCES episodes (eid),
            FOREIGN KEY (gid) REFERENCES groups (gid)
        )",
        params![],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS groups (
            gid                 INTEGER PRIMARY KEY,
            rating              INTEGER,
            votes               INTEGER,
            acount              INTEGER,
            fcount              INTEGER,
            name                TEXT,
            short               TEXT,
            irc_channel         TEXT,
            irc_server          TEXT,
            url                 TEXT,
            picname             TEXT,
            foundeddate         INTEGER,
            disbandeddate       INTEGER,
            dateflags           INTEGER,
            lastreleasedate     INTEGER,
            lastactivitydate    INTEGER,
            grouprelations      TEXT
        )",
        params![],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS indexed_files (
            path                TEXT PRIMARY KEY,
            filename            TEXT,
            filesize            INTEGER,
            fid                 INTEGER,

            UNIQUE (filename, filesize) ON CONFLICT REPLACE,
            FOREIGN KEY (fid) REFERENCES files (fid)
        )",
        params![],
    )?;

    Ok(conn)
}

struct CachedFacade<'a> {
    anidb: &'a mut AniDb,
    conn: &'a mut rusqlite::Connection,
}

macro_rules! simple_cache {
    (
        $funname:ident -> $tablename:literal ($idx:literal) -> $ranidbfun:ident -> $funret:ident,
        $questionmarks:literal:
        $($field:ident,)*
    ) => {
        async fn $funname(&mut self, id: u32) -> Result<ranidb::$funret> {
            let cached =
                self.conn
                    .query_row(
                        concat!("SELECT * FROM ", $tablename, " WHERE ", $idx, " = ?;"),
                        &[&id],

                        #[allow(unused_assignments)] // last `n` increment is unused
                        |row| {
                            let mut n = 0;
                            Ok(ranidb::$funret {
                                $( $field: row.get({ let ret = n; n += 1; ret })?, )*
                            })
                        });

            if let Ok(hit) = cached {
                log::debug!("found in cache");

                Ok(hit)
            }
            else {
                let live = self
                    .anidb
                    .$ranidbfun(id)
                    .await
                    .context("failed to get info")?;

                self.conn
                    .execute(
                        concat!("INSERT OR REPLACE INTO ", $tablename, " VALUES ", $questionmarks),
                        params![
                            $( &live.$field, )*
                        ],
                    )
                    .context("failed to store item")?;

                Ok(live)
            }
        }
    };
}

impl<'a> CachedFacade<'a> {
    fn new(anidb: &'a mut AniDb, conn: &'a mut rusqlite::Connection) -> Self {
        Self { anidb, conn }
    }

    simple_cache! {
        get_anime -> "anime"("aid") -> anime_by_id -> Anime,
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)":
            aid, dateflags, year, atype, related_aid_list, related_aid_type, romaji_name,
            kanji_name, english_name, short_name_list, episodes, special_ep_count, air_date,
            end_date, picname, nsfw, characterid_list, specials_count, credits_count, other_count,
            trailer_count, parody_count,
    }

    simple_cache! {
        get_episode -> "episodes"("eid") -> episode_by_id -> Episode,
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)":
            eid, aid, length, rating, votes, epno, eng, romaji, kanji, aired, etype,
    }

    simple_cache! {
        get_group -> "groups"("gid") -> group_by_id -> Group,
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)":
            gid, rating, votes, acount, fcount, name, short, irc_channel, irc_server, url, picname,
            foundeddate, disbandeddate, dateflags, lastreleasedate, lastactivitydate, grouprelations,
    }

    async fn get_file(&mut self, path: &Path, pb: &ProgressBar) -> Result<Option<ranidb::File>> {
        let fid: Option<(u32, String)> = self
            .conn
            .query_row(
                "SELECT fid, path FROM indexed_files WHERE path = ? OR (filename = ? AND filesize = ?);",
                params![
                    &path.to_string_lossy(),
                    &path.file_name().unwrap_or_default().to_string_lossy(),
                    path.metadata().map(|f| {
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::MetadataExt;
                            f.size() as i64
                        }

                        #[cfg(windows)]
                        {
                            use std::os::windows::fs::MetadataExt;
                            f.file_size() as i64
                        }

                        #[cfg(not(any(unix, windows)))]
                        -1
                    }).unwrap_or_default(),
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();

        if let Some((fid, indexed_path)) = fid {
            log::info!(
                "found in cache: {}",
                path.file_name()
                    .expect("invalid filename")
                    .to_string_lossy()
            );

            if indexed_path != path.to_string_lossy() {
                self.conn
                    .execute("UPDATE indexed_files SET path = ? WHERE path = ?", params![
                        &path.to_string_lossy(),
                        &indexed_path
                    ])
                    .unwrap();
            }

            Ok(self
                .conn
                .query_row("SELECT * FROM files WHERE fid = ?;", [fid], |row| {
                    Ok(ranidb::File {
                        fid: row.get(0)?,
                        aid: row.get(1)?,
                        eid: row.get(2)?,
                        gid: row.get(3)?,
                        state: row.get(4)?,
                        size: row.get(5)?,
                        ed2k: row.get(6)?,
                        colour_depth: row.get(7)?,
                        quality: row.get(8)?,
                        source: row.get(9)?,
                        audio_codec_list: row.get(10)?,
                        audio_bitrate_list: row.get(11)?,
                        video_codec: row.get(12)?,
                        video_bitrate: row.get(13)?,
                        video_resolution: row.get(14)?,
                        dub_language: row.get(15)?,
                        sub_language: row.get(16)?,
                        length_in_seconds: row.get(17)?,
                        description: row.get(18)?,
                        aired_date: row.get(19)?,
                    })
                })
                .ok())
        } else {
            let file = File::open(path).expect("opening file");
            let size = file.metadata().expect("metadata").len();

            log::info!("hashing {}...", path.to_string_lossy());

            let ed2k = format!(
                "{:032x}",
                u128::from_be_bytes(ed2k_hash(&file, pb).context("failed to hash")?)
            );

            let file = match self.anidb.file_by_ed2k(size, &ed2k).await {
                Ok(file) => file,
                Err(ranidb::Error::AniDb(ranidb::responses::Error::Other(320, _))) => {
                    return Ok(None)
                }
                e => panic!("failed to get file info: {:?}", e),
            };

            self.conn
                .execute(
                    "INSERT OR REPLACE INTO files VALUES
                        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    params![
                        &file.fid,
                        &file.aid,
                        &file.eid,
                        &file.gid,
                        &file.state,
                        &file.size,
                        &file.ed2k,
                        &file.colour_depth,
                        &file.quality,
                        &file.source,
                        &file.audio_codec_list,
                        &file.audio_bitrate_list,
                        &file.video_codec,
                        &file.video_bitrate,
                        &file.video_resolution,
                        &file.dub_language,
                        &file.sub_language,
                        &file.length_in_seconds,
                        &file.description,
                        &file.aired_date,
                    ],
                )
                .expect("failed to store file");

            self.conn
                .execute("INSERT OR REPLACE INTO indexed_files VALUES (?, ?, ?, ?)", params![
                    &path.to_string_lossy(),
                    &path.file_name().unwrap_or_default().to_string_lossy(),
                    &path
                        .metadata()
                        .map(|f| {
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::MetadataExt;
                                f.size() as i64
                            }

                            #[cfg(windows)]
                            {
                                use std::os::windows::fs::MetadataExt;
                                f.file_size() as i64
                            }

                            #[cfg(not(any(unix, windows)))]
                            -1
                        })
                        .unwrap_or_default(),
                    &file.fid
                ])
                .expect("failed to store indexed file");

            Ok(Some(file))
        }
    }
}

pub(crate) async fn index(path: &Path, db_path: &Path) -> Result<()> {
    let mut conn = init_database(db_path)?;

    let mut anidb = AniDb::new("tetsu", 1);

    anidb
        .auth("darkwater_", &std::env::var("PASS").unwrap())
        .await
        .expect("failed login");

    log::info!("session key: {}", anidb.session_key().unwrap());

    let mut facade = CachedFacade::new(&mut anidb, &mut conn);

    let mpb = MultiProgress::new();

    let mut dirs = vec![path.to_owned()];
    let mut files = vec![];

    let overall_style = ProgressStyle::default_bar()
        .progress_chars("== ")
        .template("[{elapsed_precise}] [{bar:32.cyan/blue}] [{eta:.yellow}] {pos:.green}/{len:.blue} {wide_msg}")
        .unwrap();

    let file_style = ProgressStyle::default_bar()
        .progress_chars("== ")
        .template("[{elapsed_precise}] [{bar:32.cyan/blue}] {spinner:.green} {wide_msg}")
        .unwrap();

    let overall = mpb.add(ProgressBar::new(0));
    // overall.enable_steady_tick(Duration::from_millis(125));
    overall.set_style(overall_style);
    overall.set_message("Building file list...");

    while let Some(dir) = dirs.pop() {
        let mut rd = fs::read_dir(dir).await.unwrap();
        while let Some(entry) = rd.next_entry().await.unwrap() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                files.push(path);
                overall.inc_length(1);
            }
        }
    }

    overall.reset_eta();
    overall.set_message("Indexing files...");

    for file_path in files {
        let size = fs::metadata(&file_path).await.unwrap().len() / 1024;

        let pb = mpb.insert_before(&overall, ProgressBar::new(size));
        pb.enable_steady_tick(Duration::from_millis(125));
        pb.set_style(file_style.clone());
        pb.set_message(
            file_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        );

        if let Some(file) = facade
            .get_file(&file_path, &pb)
            .await
            .context("failed to get file")?
        {
            log::debug!("file: {:#?}", file);

            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] {spinner:.green} {wide_msg:.green}")
                    .unwrap(),
            );

            pb.set_message("Getting anime info...");

            let anime = facade.get_anime(file.aid).await;
            log::debug!("anime: {:#?}", anime);

            let anime_name = anime
                .as_ref()
                .map(|a| a.romaji_name.as_str())
                .unwrap_or("Unknown");

            pb.set_message(anime_name.to_string());

            let episode = facade.get_episode(file.eid).await;
            log::debug!("episode: {:#?}", episode);

            let episode_number = episode.as_ref().map(|e| e.epno.as_str()).unwrap_or("??");

            pb.set_message(format!("{anime_name} - {episode_number}"));

            let group = facade.get_group(file.gid).await;
            log::debug!("group: {:#?}", group);

            let group_name = group.as_ref().map(|g| g.name.as_str()).unwrap_or("Unknown");

            pb.set_message(format!("{anime_name} - {episode_number} [{group_name}]"));
        } else {
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] {spinner:.green} {wide_msg:.yellow}")
                    .unwrap(),
            );

            pb.set_message(format!("Not found: {}", file_path.display()));
        }

        overall.inc(1);
        pb.finish();
    }

    overall.finish_with_message("Done!");

    {
        let mut stmt = conn.prepare("SELECT path FROM indexed_files;").unwrap();
        let indexed_files = stmt
            .query_map(params![], |row| row.get::<_, String>(0))
            .unwrap();

        let mut del_stmt = conn
            .prepare("DELETE FROM indexed_files WHERE path = ?;")
            .unwrap();
        for path in indexed_files {
            let path = path.unwrap();
            if !Path::new(path.as_str()).exists() {
                log::info!("deleting {} from index", path);
                del_stmt.execute([path]).unwrap();
            }
        }
    }

    anidb.logout().await.expect("failed logout");

    Ok(())
}
