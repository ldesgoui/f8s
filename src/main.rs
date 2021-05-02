//  mountpoint
//      .not-namespaced
//          default.ns.yaml
//          kube-system.ns.yaml
//      default
//      kube-system
//          coredns.deploy.yaml
//          coredns-74ff55c5b.rs.yaml
//          coredns-74ff55c5b-xjt82.pod.yaml

#![allow(unused)]

use polyfuse::{
    op,
    reply::{AttrOut, EntryOut, FileAttr, ReaddirOut},
    KernelConfig, Operation, Request, Session,
};
use std::path::Path;
use std::time::Duration;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    pretty_env_logger::try_init_custom_env("F8S_LOG")?;

    let mountpoint = Path::new("./mnt");
    eyre::ensure!(mountpoint.is_dir(), "./mnt must be a dir");

    let mut config = KernelConfig::default();

    // polyfuse hardcodes `/usr/bin/fusermount`
    #[cfg(feature = "polyfuse-undocumented")]
    config.fusermount_path("/run/wrappers/bin/fusermount");

    let session = Session::mount(mountpoint.into(), config)?;

    let client = kube::Client::try_default().await?;

    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    while let Some(req) = session.next_request()? {
        log::debug!("request {:#x}:", req.unique());
        log::debug!("  - uid: {:?}", req.uid());
        log::debug!("  - gid: {:?}", req.gid());
        log::debug!("  - pid: {:?}", req.pid());

        let op = req.operation()?;
        log::debug!("  - op: {:?}", op);

        match op {
            Operation::Getattr(op) if op.ino() == 1 => {
                log::info!("getattr on mountpoint");

                let mut out = AttrOut::default();
                let attrs = out.attr();

                attrs.ino(1);
                attrs.mode(libc::S_IFDIR | libc::S_IRUSR | libc::S_IXUSR);
                attrs.nlink(1); // TODO: idk what this is supposed to be :)
                attrs.uid(uid);
                attrs.gid(gid);
                out.ttl(Duration::from_secs(1));

                req.reply(out)?;
            }

            Operation::Getattr(op) => {
                log::debug!("    - ino: {:?}", op.ino());
                log::debug!("    - fh: {:?}", op.fh());

                log::warn!("getattr on something not found");
                req.reply_error(libc::ENOENT)?;
            }

            Operation::Readdir(op) if op.ino() == 1 => {
                log::info!("readdir on mountpoint");

                let mut out = ReaddirOut::new(0);

                // TODO: am I supposed to give `.` and `..`?

                req.reply(out)?;
            }

            Operation::Readdir(op) => {
                log::debug!("    - ino: {:?}", op.ino());
                log::debug!("    - fh: {:?}", op.fh());
                log::debug!("    - offset: {:?}", op.offset());
                log::debug!("    - size: {:?}", op.size());
                log::debug!("    - mode: {:?}", op.mode());

                log::warn!("readdir on something not found");
                req.reply_error(libc::ENOENT)?;
            }

            Operation::Read(op) if op.ino() == 1 => {
                log::warn!("read on mountpoint");
                req.reply_error(libc::EISDIR)?;
            }

            Operation::Read(op) => {
                log::info!("    - ino: {:?}", op.ino());
                log::info!("    - fh: {:?}", op.fh());
                log::info!("    - offset: {:?}", op.size());
                log::info!("    - size: {:?}", op.size());
                log::info!("    - flags: {:?}", op.flags());
                log::info!("    - lock_owner: {:?}", op.lock_owner());

                log::warn!("read on something not found");
                req.reply_error(libc::ENOENT)?;
            }

            // Operation::Lookup(op) => fs.lookup(&req, op)?,
            _ => req.reply_error(libc::ENOSYS)?,
        }
    }

    Ok(())
}

/*
enum Entry {
    // The mountpoint is a directory
    Mountpoint,
    // For each namespace exists a directory
    NamespaceDir { name: String },
    // There exists a directory for non-namespaced values
    NotNamespacedDir,
}
*/
