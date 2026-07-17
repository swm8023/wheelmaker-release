# wheelmaker-release

Public release channel for WheelMaker deployment artifacts.

## Install or migrate

Install [Node.js 22 or newer](https://nodejs.org/) first. The command for your
platform downloads the public deployment launcher, removes a legacy WheelMaker
runtime when present, and installs the current stable release.

Each command is a single line that can be pasted from any current working
directory. It always installs into the current user's `~/.wheelmaker` directory
and does not require a WheelMaker source checkout. Run it as the target user,
not with `sudo`.

### Windows PowerShell

```powershell
$ErrorActionPreference='Stop'; $d=Join-Path $HOME '.wheelmaker'; New-Item -ItemType Directory -Force -Path $d | Out-Null; $m=Join-Path $d 'deploy.mjs'; Invoke-WebRequest 'https://raw.githubusercontent.com/swm8023/wheelmaker-release/main/deploy.mjs' -OutFile $m; & node $m migrate-uninstall; if ($LASTEXITCODE -eq 0) { & node $m }
```

Windows requests UAC only when legacy Windows services must be removed.

### macOS or Linux shell

```sh
(d="$HOME/.wheelmaker" && mkdir -p "$d" && curl --fail --location --proto '=https' --tlsv1.2 'https://raw.githubusercontent.com/swm8023/wheelmaker-release/main/deploy.mjs' --output "$d/deploy.mjs" && node "$d/deploy.mjs" migrate-uninstall && node "$d/deploy.mjs")
```

Migration preserves WheelMaker configuration, databases, logs, Desktop files,
and other user data. Running the command on a machine without a legacy runtime
performs a normal new installation.
