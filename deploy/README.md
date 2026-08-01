# Deployment

**Canonical host:** [https://channelzeronews.app](https://channelzeronews.app)

Legacy aliases permanently redirect there:

- `zero.stephens.page`
- `channelzeronews.stephens.page`
- `channel0.stephens.page`
- `channelzeronews.stewardgoods.com`
- `www.channelzeronews.app` → apex

All traffic is reverse-proxied (with WebSocket upgrade) to the one `channel-zero`
systemd service on `127.0.0.1:3471`.

## Pieces

* **systemd**: `channel-zero.service` runs the release binary as `jacob`, binds
  `127.0.0.1:3471`, loads `DATABASE_URL` from
  `/var/www/channel0.stephens.page/.env`. Working directory remains
  `/var/www/channel0.stephens.page` (app tree path, not the public hostname).
* **Apache**: `channelzeronews.app.apache.conf` + `channelzeronews.app-le-ssl.conf`
  are the primary vhosts. Certbot manages TLS under
  `/etc/letsencrypt/live/channelzeronews.app/`.

## Apply / update

```bash
# build
CARGO_TARGET_DIR=/mnt/volume_nyc3_01/jacob/channel0-target cargo build --release

# install service (first time)
sudo cp deploy/channel-zero.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now channel-zero

# primary vhost (first time)
sudo cp deploy/channelzeronews.app.apache.conf /etc/apache2/sites-available/channelzeronews.app.conf
sudo a2ensite channelzeronews.app.conf
sudo apache2ctl configtest && sudo systemctl reload apache2
sudo certbot --apache -d channelzeronews.app -d www.channelzeronews.app \
  --non-interactive --redirect -m jacob@stephens.page
# then ensure the SSL vhost still reverse-proxies (see channelzeronews.app-le-ssl.conf)

# redeploy after a code change
CARGO_TARGET_DIR=/mnt/volume_nyc3_01/jacob/channel0-target cargo build --release
sudo systemctl restart channel-zero
```

## Required Apache modules

`proxy proxy_http proxy_wstunnel rewrite headers ssl` (all enabled).
