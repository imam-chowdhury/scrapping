# Ubuntu Docker Deploy (wire.automatebd.xyz)

This deploys:

- `app` (Flask + scheduler) on an internal port
- `caddy` as the public reverse-proxy on `80/443` with TLS

## 1) DNS

Create an `A` record:

- `wire.automatebd.xyz` -> your Ubuntu server public IP

## 2) Server prerequisites

Install Docker + Compose plugin (Ubuntu 22.04+):

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
```

Log out and log back in (or reboot) so the group change applies.

Open firewall:

```bash
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
```

## 3) SSL certificates ("local SSL")

Create `ssl/` and place your certificate files:

- `ssl/fullchain.pem`
- `ssl/privkey.pem`

If you use Cloudflare, you can generate a Cloudflare Origin Certificate for `wire.automatebd.xyz` and save it as those two files.

Quick self-signed (for testing only; browsers will warn):

```bash
mkdir -p ssl
openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
  -keyout ssl/privkey.pem -out ssl/fullchain.pem \
  -subj "/CN=wire.automatebd.xyz"
```

## 4) Run

From the repo folder:

```bash
docker compose up -d --build
```

Check status/logs:

```bash
docker compose ps
docker compose logs -f caddy
docker compose logs -f app
```

## 5) Verify

API:

```bash
curl -k https://scrap.automatebd.xyz/api/news
curl -k https://wire.automatebd.xyz/api/news
```

Dashboard:

- `https://wire.automatebd.xyz/`

## Update

```bash
git pull
docker compose up -d --build
```
