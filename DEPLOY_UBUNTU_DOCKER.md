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

## 3) SSL (Trusted, no browser warning)

Caddy will automatically issue a trusted TLS certificate (Let's Encrypt) as long as:

- `wire.automatebd.xyz` DNS points to this server
- Ports `80` and `443` are reachable from the internet

If you are using Cloudflare proxy (orange cloud), set SSL mode to **Full (strict)** after Caddy issues the cert.

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
curl -I https://wire.automatebd.xyz/api/news
```

Dashboard:

- `https://wire.automatebd.xyz/`

## Update

```bash
git pull
docker compose up -d --build
```
