//  mountpoint
//      .not-namespaced
//          default.ns.yaml
//          kube-system.ns.yaml
//      default
//      kube-system
//          coredns.deploy.yaml
//          coredns-0000.rs.yaml
//          coredns-0000-00000000.pod.yaml

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
    tracing_subscriber::fmt::init();
    // pretty_env_logger::try_init_custom_env("F8S_LOG")?;

    let mountpoint = Path::new("./mnt");
    eyre::ensure!(mountpoint.is_dir(), "./mnt must be a dir");

    let mut config = KernelConfig::default();
    // polyfuse hardcodes `/usr/bin/fusermount`
    config.fusermount_path("/run/wrappers/bin/fusermount");

    let session = Session::mount(mountpoint.into(), config)?;

    let client = kube::Client::try_default().await?;

    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    while let Some(req) = session.next_request()? {
        log::debug!("request {:#x}:", req.unique());
        log::debug!("  - uid: {}", req.uid());
        log::debug!("  - gid: {}", req.gid());
        log::debug!("  - pid: {}", req.pid());

        let op = req.operation()?;
        log::debug!("  - op: {:?}", op);

        match op {
            Operation::Getattr(op) => {
                log::debug!("    - ino: {}", op.ino());
                log::debug!("    - fh: {:?}", op.fh());

                if op.ino() == 1 {
                    log::info!("getattr on mountpoint");

                    let mut out = AttrOut::default();
                    let attrs = out.attr();

                    attrs.ino(1);
                    attrs.mode(libc::S_IFREG as u32);
                    attrs.size(1);
                    attrs.nlink(1);
                    attrs.uid(uid);
                    attrs.gid(gid);

                    out.ttl(Duration::from_secs(1));

                    req.reply(out)?;
                    continue;
                }

                log::warn!("getattr on something not found");
                req.reply_error(libc::ENOENT)?;
            }
            Operation::Read(op) => {
                log::info!("    - ino: {}", op.ino());
                log::info!("    - fh: {}", op.fh());
                log::info!("    - offset: {}", op.size());
                log::info!("    - size: {}", op.size());
                log::info!("    - flags: {}", op.flags());
                log::info!("    - lock_owner: {:?}", op.lock_owner());

                if op.ino() == 1 {
                    log::info!("read on mountpoint");
                    req.reply(&[])?;
                    continue;
                }

                log::warn!("read on something not found");
                req.reply_error(libc::ENOENT)?;
            }

            // Operation::Lookup(op) => fs.lookup(&req, op)?,
            // Operation::Readdir(op) => fs.readdir(&req, op)?,
            _ => req.reply_error(libc::ENOSYS)?,
        }
    }

    Ok(())
}
