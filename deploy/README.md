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

## Room quota keys on the address Apache appends

`POST /api/rooms` allows 8 live rooms per client address, and the client
address is the entry the local Apache appends to `X-Forwarded-For`. mod_proxy
adds that header because `ProxyAddHeaders` defaults to On and none of these
vhosts turn it off, so the Apache config needs no change. The service reads
the header only from a loopback peer, takes its rightmost entry, and ignores
any entries the client sent in front of it.

* Keep `CHANNEL_ZERO_ADDR` on `127.0.0.1` behind Apache. A request from any
  other peer counts against that peer's own address, forwarded headers ignored.
* Keep `ProxyAddHeaders` on. With it Off, Apache stops appending but still
  forwards any `X-Forwarded-For` the client sent, so the rightmost entry is the
  client's own choice: a client could name a new address on every create and
  skip the quota. Clients that send no header would share 127.0.0.1's quota.
* Another proxy in front of Apache would key every create on that proxy's
  address, so all clients behind it would share one 8-room quota.

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
