# opush - a UnifiedPush destributor for the Linux Desktop
opush uses [autopush-rs](https://github.com/mozilla-services/autopush-rs/) by Mozilla as the delivery channel for
push notifications on the linux desktop and provides the DBus methods specified by the [UnifiedPush](https://unifiedpush.org/) specification.

## Config
The config file of opush is located at `$XDG_CONFIG_HOME/opush.json` (`~/.config/opush.json` if no other config home was specified).
The format has to be the following `{"url":"wss://<URL of push-server>"}` with
the URL pointing at a [autopush-rs](https://github.com/mozilla-services/autopush-rs/) deployment.

The application state of opush will be stored at `$XDG_STATE_HOME/opush.json` (defaulting to `~/.local/state/opush.json`).
