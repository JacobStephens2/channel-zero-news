# Deployment

Live at **https://channel0.stephens.page** (runs in parallel with the original
PHP game on zero/channelzeronews.stephens.page).

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
