#![allow(unused)]

use bimap::BiMap;
use k8s_openapi::api::core::v1::{Namespace, Node, Pod};
use polyfuse::{op, reply::*, KernelConfig, Operation, Request, Session};
use std::collections::HashMap;
use std::convert::TryFrom as _;
use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

mod async_session;

const INO_ROOT: u64 = 1;
const INO_DIR_FREESTANDING: u64 = 2;
const INO_ALLOC_START: u64 = 3;

const TTL_FOREVER: Duration = Duration::from_secs(60 * 60 * 24 * 365);
const TTL_SHORT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    pretty_env_logger::try_init_custom_env("F8S_LOG")?;

    // TODO: option
    let mountpoint = Path::new("./mnt");
    eyre::ensure!(mountpoint.is_dir(), "./mnt must be a dir");

    let mut config = KernelConfig::default();

    #[cfg(feature = "polyfuse-undocumented")]
    {
        config.mount_option("default_permissions");

        // polyfuse hardcodes `/usr/bin/fusermount`
        config.fusermount_path("/run/wrappers/bin/fusermount");
    }

    let session = Session::mount(mountpoint.into(), config)?;

    // TODO: option --context
    // TODO: option --kubeconfig
    let client = kube::Client::try_default().await?;

    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    let mut next_ino = AtomicU64::new(INO_ALLOC_START);

    let mut map = BiMap::<u64, Entry>::new();
    map.insert(INO_ROOT, Entry::Mountpoint);
    map.insert(INO_DIR_FREESTANDING, Entry::FreestandingDir);

    let mut next_fh = AtomicU64::new(10000);

    let mut opened_dirs = HashMap::<u64, Vec<(OsString, u64, u32)>>::new();
    let mut opened_files = HashMap::<u64, String>::new();

    while let Some(req) = session.next_request()? {
        log::debug!("request {:#x}:", req.unique());
        // log::debug!("  - uid: {:?}", req.uid());
        // log::debug!("  - gid: {:?}", req.gid());
        // log::debug!("  - pid: {:?}", req.pid());

        let op = req.operation()?;
        log::debug!("  - op: {:?}", op);

        match op {
            Operation::Getattr(op) if map.contains_left(&op.ino()) => {
                let entry = map.get_by_left(&op.ino()).unwrap();

                log::info!("getattr on entry: {:?}", entry);

                let mut out = AttrOut::default();
                out.ttl(TTL_SHORT);

                let attrs = out.attr();
                attrs.ino(op.ino());
                attrs.uid(uid);
                attrs.gid(gid);

                if entry.is_dir() {
                    attrs.mode(libc::S_IFDIR | libc::S_IRUSR | libc::S_IXUSR);
                    attrs.nlink(2);
                } else {
                    attrs.mode(libc::S_IFREG | libc::S_IRUSR);
                    attrs.nlink(1);
                    attrs.size(10);
                }

                req.reply(out)?;
            }

            Operation::Getattr(op) => {
                log::debug!("    - ino: {:?}", op.ino());
                log::debug!("    - fh: {:?}", op.fh());

                log::warn!("getattr, no matching inode for: {:?}", op.ino());
                req.reply_error(libc::EINVAL)?;
            }

            Operation::Lookup(op) if matches!(map.get_by_left(&op.parent()), None) => {
                log::warn!("lookup, no matching inode for parent: {}", op.parent());
                req.reply_error(libc::EINVAL)?;
            }

            Operation::Lookup(op) if op.parent() == INO_ROOT && op.name() == "_" => {
                log::info!("lookup on /_/");

                let mut out = EntryOut::default();
                out.ino(INO_DIR_FREESTANDING);
                out.ttl_attr(TTL_FOREVER);
                out.ttl_entry(TTL_FOREVER);

                let attrs = out.attr();
                attrs.ino(INO_DIR_FREESTANDING);
                attrs.mode(libc::S_IFDIR | libc::S_IRUSR | libc::S_IXUSR);
                attrs.nlink(2);
                attrs.uid(uid);
                attrs.gid(gid);

                req.reply(out)?;
            }

            Operation::Lookup(op)
                if op.parent() == INO_ROOT
                    && map.contains_right(&Entry::NamespaceDir {
                        name: op.name().to_str().unwrap().into(),
                    }) =>
            {
                let ino = map
                    .get_by_right(&Entry::NamespaceDir {
                        name: op.name().to_str().unwrap().into(),
                    })
                    .copied()
                    .unwrap();

                log::info!(
                    "lookup on namespace entry: name: {:?}, ino: {:?}",
                    op.name(),
                    ino
                );

                let mut out = EntryOut::default();
                out.ino(ino);
                out.ttl_attr(TTL_SHORT);
                out.ttl_entry(TTL_SHORT);

                let attrs = out.attr();
                attrs.ino(ino);
                attrs.mode(libc::S_IFDIR | libc::S_IRUSR | libc::S_IXUSR);
                attrs.nlink(2);
                attrs.uid(uid);
                attrs.gid(gid);

                req.reply(out)?;
            }

            Operation::Lookup(op) if op.parent() == INO_DIR_FREESTANDING => {
                let entry = match op
                    .name()
                    .to_str()
                    .unwrap()
                    .rsplitn(3, '.')
                    .collect::<Vec<_>>()
                    .as_slice()
                {
                    ["yaml", &ref kind, &ref name] => Entry::ObjectFreestanding {
                        name: name.into(),
                        kind: kind.into(),
                    },
                    _ => {
                        log::warn!("lookup on file with the wrong format: {:?}", op.name());
                        req.reply_error(libc::ENOENT)?;
                        continue;
                    }
                };

                let ino = map.get_by_right(&entry).copied().unwrap();

                log::info!("lookup on entry: {:?}", entry);

                let mut out = EntryOut::default();
                out.ino(ino);
                out.ttl_attr(TTL_SHORT);
                out.ttl_entry(TTL_SHORT);

                let attrs = out.attr();
                attrs.ino(ino);
                attrs.mode(libc::S_IFREG | libc::S_IRUSR);
                attrs.nlink(1);
                attrs.size(10);
                attrs.uid(uid);
                attrs.gid(gid);

                req.reply(out)?;
            }

            Operation::Lookup(op)
                if matches!(
                    map.get_by_left(&op.parent()),
                    Some(Entry::NamespaceDir { .. })
                ) =>
            {
                let namespace = map.get_by_left(&op.parent()).unwrap().file_name(); // TODO: no

                let entry = match op
                    .name()
                    .to_str()
                    .unwrap()
                    .rsplitn(3, '.')
                    .collect::<Vec<_>>()
                    .as_slice()
                {
                    ["yaml", &ref kind, &ref name] => Entry::ObjectNamespaced {
                        namespace,
                        name: name.into(),
                        kind: kind.into(),
                    },
                    _ => {
                        log::warn!("lookup on file with the wrong format: {:?}", op.name());
                        req.reply_error(libc::ENOENT)?;
                        continue;
                    }
                };

                let ino = map.get_by_right(&entry).copied().unwrap();

                log::info!("lookup on entry: {:?}", entry);

                let mut out = EntryOut::default();
                out.ino(ino);
                out.ttl_attr(TTL_SHORT);
                out.ttl_entry(TTL_SHORT);

                let attrs = out.attr();
                attrs.ino(ino);
                attrs.mode(libc::S_IFREG | libc::S_IRUSR);
                attrs.size(10);
                attrs.uid(uid);
                attrs.gid(gid);

                req.reply(out)?;
            }

            // TODO: {namespace} when not in map
            // TODO: _/{name}.{kind}.yaml when not in map
            // TODO: {namespace}/{name}.{kind}.yaml when not in map
            // TODO: refactor cases above
            Operation::Lookup(op) => {
                log::debug!("    - parent: {:?}", op.parent());
                log::debug!("    - name: {:?}", op.name());

                log::warn!("lookup with no matching case");
                req.reply_error(libc::ENOENT)?;
            }

            Operation::Opendir(op)
                if map
                    .get_by_left(&op.ino())
                    .map(|e| !e.is_dir())
                    .unwrap_or(false) =>
            {
                log::warn!("opendir on something else than a dir");
                req.reply_error(libc::ENOTDIR)?;
            }

            Operation::Opendir(op) if op.ino() == INO_ROOT => {
                log::info!("opendir on mountpoint");

                let fh = next_fh.fetch_add(1, Ordering::SeqCst);

                let mut entries = vec![
                    (".".into(), INO_ROOT, libc::DT_DIR as u32),
                    ("..".into(), INO_ROOT, libc::DT_DIR as u32),
                    ("_".into(), INO_DIR_FREESTANDING, libc::DT_DIR as u32),
                ];

                let namespaces = kube::Api::<Namespace>::all(client.clone());

                for ns in namespaces.list(&Default::default()).await?.items {
                    let entry = Entry::from(ns);
                    let file_name = entry.file_name();
                    let ino = map.get_by_right(&entry).copied().unwrap_or_else(|| {
                        let ino = next_ino.fetch_add(1, Ordering::SeqCst);
                        let overwritten = map.insert(ino, entry);

                        if overwritten.did_overwrite() {
                            log::error!(
                                "wrote over something in the ino/entry mapping: {:?}",
                                overwritten
                            );
                        }

                        ino
                    });

                    entries.push((OsString::from(file_name), ino, libc::DT_DIR as u32));
                }

                opened_dirs.insert(fh, entries);

                let mut out = OpenOut::default();
                out.fh(fh);
                out.direct_io(true); // TODO: no clue what this does

                req.reply(out)?;
            }

            // TODO: discovery
            Operation::Opendir(op) if op.ino() == INO_DIR_FREESTANDING => {
                log::info!("opendir on /_/");

                let fh = next_fh.fetch_add(1, Ordering::SeqCst);

                let mut entries = vec![
                    (".".into(), INO_DIR_FREESTANDING, libc::DT_DIR as u32),
                    ("..".into(), INO_ROOT, libc::DT_DIR as u32),
                ];

                let namespaces = kube::Api::<Namespace>::all(client.clone());

                // TODO: data needed here
                for ns in namespaces.list(&Default::default()).await?.items {
                    let name = ns.metadata.name.unwrap();
                    let entry = Entry::ObjectFreestanding {
                        name: name.clone(),
                        kind: "ns".into(),
                    };
                    let file_name = entry.file_name();
                    let ino = map.get_by_right(&entry).copied().unwrap_or_else(|| {
                        let ino = next_ino.fetch_add(1, Ordering::SeqCst);
                        let overwritten = map.insert(ino, entry);

                        if overwritten.did_overwrite() {
                            log::error!(
                                "wrote over something in the ino/entry mapping: {:?}",
                                overwritten
                            );
                        }

                        ino
                    });

                    entries.push((OsString::from(file_name), ino, libc::DT_REG as u32));
                }

                let nodes = kube::Api::<Node>::all(client.clone());

                for no in nodes.list(&Default::default()).await?.items {
                    let name = no.metadata.name.unwrap();
                    let entry = Entry::ObjectFreestanding {
                        name: name.clone(),
                        kind: "no".into(),
                    };
                    let file_name = entry.file_name();
                    let ino = map.get_by_right(&entry).copied().unwrap_or_else(|| {
                        let ino = next_ino.fetch_add(1, Ordering::SeqCst);
                        let overwritten = map.insert(ino, entry);

                        if overwritten.did_overwrite() {
                            log::error!(
                                "wrote over something in the ino/entry mapping: {:?}",
                                overwritten
                            );
                        }

                        ino
                    });

                    entries.push((OsString::from(file_name), ino, libc::DT_REG as u32));
                }

                opened_dirs.insert(fh, entries);

                let mut out = OpenOut::default();
                out.fh(fh);
                out.direct_io(true); // TODO: no clue what this does

                req.reply(out)?;
            }

            // TODO: discovery
            Operation::Opendir(op) if map.contains_left(&op.ino()) => {
                let entry = map.get_by_left(&op.ino()).unwrap();

                log::info!("opendir on entry: {:?}", entry);

                let namespace = entry.file_name(); // TODO: no

                let fh = next_fh.fetch_add(1, Ordering::SeqCst);

                let mut entries = vec![
                    (".".into(), op.ino(), libc::DT_DIR as u32),
                    ("..".into(), INO_ROOT, libc::DT_DIR as u32),
                ];

                // TODO: data needed here
                let pods = kube::Api::<Pod>::namespaced(client.clone(), &namespace);

                for po in pods.list(&Default::default()).await?.items {
                    let entry = Entry::ObjectNamespaced {
                        namespace: namespace.clone(),
                        name: po.metadata.name.as_ref().unwrap().clone(),
                        kind: "po".into(),
                    };
                    let file_name = entry.file_name();
                    let ino = map.get_by_right(&entry).copied().unwrap_or_else(|| {
                        let ino = next_ino.fetch_add(1, Ordering::SeqCst);
                        let overwritten = map.insert(ino, entry);

                        if overwritten.did_overwrite() {
                            log::error!(
                                "wrote over something in the ino/entry mapping: {:?}",
                                overwritten
                            );
                        }

                        ino
                    });

                    entries.push((OsString::from(file_name), ino, libc::DT_DIR as u32));
                }

                opened_dirs.insert(fh, entries);

                let mut out = OpenOut::default();
                out.fh(fh);
                out.direct_io(true); // TODO: no clue what this does

                req.reply(out)?;
            }

            // TODO: refactor cases above
            Operation::Opendir(op) => {
                log::debug!("    - ino: {:?}", op.ino());
                log::debug!("    - flags: {:?}", op.flags());

                log::warn!("opendir with no matching case");
                req.reply_error(libc::ENOENT)?;
            }

            Operation::Readdir(op) if op.mode() == op::ReaddirMode::Plus => {
                log::warn!("readdir+");

                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Readdir(op) if opened_dirs.contains_key(&op.fh()) => {
                log::info!("readdir on opened dir: fh: {:?}", op.fh());
                log::debug!("offset: {:?}", op.offset());

                let entries = opened_dirs.get(&op.fh()).unwrap();

                let mut out = ReaddirOut::new(op.size() as usize);

                for (offset, entry) in entries.iter().enumerate().skip(op.offset() as usize) {
                    let filled = out.entry(&entry.0, entry.1, entry.2, offset as u64 + 1);

                    if filled {
                        break;
                    };
                }

                req.reply(out)?;
            }

            // TODO
            Operation::Readdir(op) => {
                log::debug!("    - ino: {:?}", op.ino());
                log::debug!("    - fh: {:?}", op.fh());
                log::debug!("    - offset: {:?}", op.offset());
                log::debug!("    - size: {:?}", op.size());
                log::debug!("    - mode: {:?}", op.mode());

                log::warn!("readdir with no matching case");
                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Releasedir(op) if opened_dirs.contains_key(&op.fh()) => {
                log::info!("releasing dir: fh: {:?}", op.fh());
                opened_dirs.remove(&op.fh());
            }

            Operation::Releasedir(op) => {
                log::debug!("    - fh: {:?}", op.fh());
                log::debug!("    - flags: {:?}", op.flags());

                log::info!("releasedir with no matching case");
                req.reply_error(libc::EBADF)?;
            }

            Operation::Open(op)
                if map
                    .get_by_left(&op.ino())
                    .map(Entry::is_dir)
                    .unwrap_or(false) =>
            {
                log::warn!("open on a directory");
                req.reply_error(libc::EISDIR)?;
            }

            Operation::Open(op)
                if map
                    .get_by_left(&op.ino())
                    .map(Entry::is_dir)
                    .unwrap_or(false) =>
            {
                log::warn!("open on a directory");
                req.reply_error(libc::EISDIR)?;
            }

            Operation::Open(op)
                if matches!(
                    map.get_by_left(&op.ino()),
                    Some(Entry::ObjectFreestanding { .. })
                ) =>
            {
                let entry = map.get_by_left(&op.ino()).unwrap();

                log::info!("open on entry: {:?}", entry);

                let fh = next_fh.fetch_add(1, Ordering::SeqCst);
                let contents = "HELLO LMAO".into();

                opened_files.insert(fh, contents);

                let mut out = OpenOut::default();

                out.fh(fh);
                out.direct_io(true); // TODO: still nto sure what this does but it makes thigns work

                req.reply(out)?;
            }

            // TODO: _/{name}.{kind}.yaml
            // TODO: {namespace}/{name}.{kind}.yaml
            // TODO: flags
            Operation::Open(op) => {
                log::info!("    - ino: {:?}", op.ino());
                log::info!("    - flags: {:?}", op.flags());

                log::warn!("open with no matching case");
                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Read(op) if opened_files.contains_key(&op.fh()) => {
                let contents = opened_files.get(&op.fh()).unwrap().as_bytes();

                log::info!("read on fh: {:?}", op.fh());

                let mut out: &[u8] = &[];

                let offset = op.offset() as usize;

                if offset < contents.len() {
                    let size = op.size() as usize;
                    out = &contents[offset..];
                    out = &out[..std::cmp::min(out.len(), size)];
                }

                dbg!(out);

                req.reply(out);
            }

            // TODO
            Operation::Read(op) => {
                log::info!("    - ino: {:?}", op.ino());
                log::info!("    - fh: {:?}", op.fh());
                log::info!("    - offset: {:?}", op.offset());
                log::info!("    - size: {:?}", op.size());
                log::info!("    - flags: {:?}", op.flags());
                log::info!("    - lock_owner: {:?}", op.lock_owner());

                log::warn!("read with no matching case");
                req.reply_error(libc::EINVAL)?; // XXX: not ENOSYS cus of no-open support
            }

            Operation::Release(op) if opened_files.contains_key(&op.fh()) => {
                log::info!("releasing file: fh: {:?}", op.fh());
                opened_files.remove(&op.fh());
            }

            Operation::Release(op) => {
                log::info!("    - ino: {:?}", op.ino());
                log::info!("    - fh: {:?}", op.fh());
                log::info!("    - flags: {:?}", op.flags());
                log::info!("    - lock_owner: {:?}", op.lock_owner());
                log::info!("    - flush: {:?}", op.flush());
                log::info!("    - flock_release: {:?}", op.flock_release());

                log::warn!("release with no matching case");
                req.reply_error(libc::EINVAL)?;
            }

            _ => req.reply_error(libc::ENOSYS)?,
        }
    }

    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum Entry {
    // /
    Mountpoint,

    // /_/
    FreestandingDir,

    // /_/{name}.{kind}.yaml
    ObjectFreestanding {
        name: String,
        kind: String,
    },

    // /{name}/
    NamespaceDir {
        name: String,
    },

    // /{namespace}/{name}.{kind}.yaml
    ObjectNamespaced {
        namespace: String,
        name: String,
        kind: String,
    },
}

impl Entry {
    fn is_dir(&self) -> bool {
        use Entry::*;

        matches!(self, Mountpoint | FreestandingDir | NamespaceDir { .. })
    }

    fn is_regular(&self) -> bool {
        use Entry::*;

        matches!(self, ObjectFreestanding { .. } | ObjectNamespaced { .. })
    }

    fn file_name(&self) -> String {
        use Entry::*;

        match self {
            Mountpoint => unreachable!(),

            FreestandingDir => "_".into(),

            NamespaceDir { name } => name.into(),

            ObjectNamespaced { name, kind, .. } | ObjectFreestanding { name, kind } => {
                format!("{}.{}.yaml", name, kind)
            }
        }
    }
}

impl From<Namespace> for Entry {
    fn from(ns: Namespace) -> Self {
        Self::NamespaceDir {
            name: ns.metadata.name.unwrap(),
        }
    }
}

impl From<&'_ Namespace> for Entry {
    fn from(ns: &Namespace) -> Self {
        Self::NamespaceDir {
            name: ns.metadata.name.as_ref().cloned().unwrap(),
        }
    }
}
