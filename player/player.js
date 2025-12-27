const player = videojs('videoPlayer');

// Channel list → MPD URLs served by FastAPI
const channels = {
  "DR1": "https://kodi.lyhnemail.com/streamer/stream/798254152/manifest.mpd",
  "DR1_alt": "https://kodi.lyhnemail.com/streamer/stream/798254152/manifest.mpd"
};

// Load initial channel
player.src({
  src: channels["DR1"],
  type: "application/dash+xml"
});

// ---- Channel Menu ----
const MenuButton = videojs.getComponent('MenuButton');
const MenuItem   = videojs.getComponent('MenuItem');

class ChannelMenuItem extends MenuItem {
  constructor(player, options) {
    super(player, options);

    this.on('click', () => {
      player.pause();
      player.src({
        src: options.src,
        type: "application/dash+xml"
      });
      player.play();
    });
  }
}

class ChannelMenuButton extends MenuButton {
  constructor(player, options) {
    super(player, options);
    this.controlText("Channels");
  }

  createItems() {
    return Object.entries(channels).map(([label, src]) =>
      new ChannelMenuItem(this.player_, {
        label,
        src
      })
    );
  }
}

videojs.registerComponent('ChannelMenuButton', ChannelMenuButton);

// Add menu to control bar
player.ready(() => {
  player.getChild('controlBar')
        .addChild('ChannelMenuButton', {}, 10);
});
