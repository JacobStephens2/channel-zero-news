# Deployment

Live (canonical) at **https://channelzeronews.stephens.page**, also on
**channelzeronews.stewardgoods.com**, **zero.stephens.page**, and
**channel0.stephens.page** — all reverse-proxied to the one `channel-zero`
service. The production domains were **cut over from the old PHP app** on
2026-05-29 (their `:443` vhosts repointed from `DocumentRoot /var/www/zero...`
to the proxy; see `channelzeronews.stephens.page-le-ssl.conf` here for the shape).
A self-unregistering `static/sw.js` clears the old PWA service worker so returning
visitors load the new client.

### Revert the cutover

The original vhosts were backed up before editing (path printed during cutover,
under `/tmp/cz-cutover-backup-*`). To roll back a domain, restore its
`*-le-ssl.conf.bak` to `/etc/apache2/sites-available/<domain>-le-ssl.conf`,
`apache2ctl configtest`, and `systemctl reload apache2`. The PHP app and its
MySQL data are untouched.

## Pieces

* **systemd**: `channel-zero.service` runs the release binary as `jacob`, binds
  `127.0.0.1:3471`, loads `DATABASE_URL` from `../.env`. Postgres data dir lives
  on the mounted volume (`/mnt/volume_nyc3_01/jacob/pgdata`) since the root disk
  is near-full.
* **Apache**: `channel0.stephens.page.apache.conf` reverse-proxies HTTP to the
  service and upgrades `Upgrade: websocket` requests to the `ws://` backend via
  `mod_proxy_wstunnel`. TLS + the HTTP→HTTPS redirect were added by
  `certbot --apache` (generates the `-le-ssl.conf`).

## Apply / update

```bash
# build
CARGO_TARGET_DIR=/mnt/volume_nyc3_01/jacob/channel0-target cargo build --release

# install service (first time)
sudo cp deploy/channel-zero.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now channel-zero

# install vhost (first time)
sudo cp deploy/channel0.stephens.page.apache.conf /etc/apache2/sites-available/channel0.stephens.page.conf
sudo a2ensite channel0.stephens.page.conf
sudo apache2ctl configtest && sudo systemctl reload apache2
sudo certbot --apache -d channel0.stephens.page --non-interactive --redirect

# redeploy after a code change
CARGO_TARGET_DIR=/mnt/volume_nyc3_01/jacob/channel0-target cargo build --release
sudo systemctl restart channel-zero
```

## Required Apache modules

`proxy proxy_http proxy_wstunnel rewrite headers ssl` (all enabled).
