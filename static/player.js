// ---------------- Video.js Player Setup ----------------
const channelsEl = document.getElementById("channels");

// Initialize Video.js player
// var options;

// options = {
//    controls: true,
//    techOrder: [ 'chromecast', 'html5' ], // You may have more Tech, such as Flash or HLS
//    plugins: {
//       chromecast: {}
//    }
// };

class ChromecastTech extends videojs.getComponent('Tech') {
  constructor(options, ready) {
    super(options, ready);
    // init your Chromecast logic here
  }

  // mandatory methods for Tech
  play() { /* send play to Chromecast */ }
  pause() { /* send pause to Chromecast */ }
  currentTime() { return 0; }
  duration() { return 0; }
  // …implement other required Tech methods
}

const player = videojs(document.getElementById('myVideoElement'));

player.getTech('Chromecast') || videojs.registerTech('Chromecast', ChromecastTech);
player.ChromecastTech = ChromecastTech;

// Initialize the chromecast plugin
// The exact initialization might vary slightly based on plugin version.
player.chromecast();

// ---------------- Load Channels ----------------
async function loadChannels() {
  const res = await fetch("/streamer/api/channels");
  const channels = await res.json();
  renderChannels(channels);
}

// ---------------- Render Sidebar ----------------
function renderChannels(channels) {
  channelsEl.innerHTML = "";

  channels.forEach(ch => {
    const div = document.createElement("div");
    div.className = "channel";
    div.innerHTML = `
      <img src="${ch.logo}">
      <span>${ch.name}</span>
    `;

    div.onclick = () => playChannel(ch.id, div);

    channelsEl.appendChild(div);
  });
}

// ---------------- Play Channel ----------------
async function playChannel(channelId, element) {
  // Highlight active channel
  document.querySelectorAll(".channel").forEach(c => c.classList.remove("active"));
  element.classList.add("active");

  // Get token for HLS stream
  const res = await fetch(`/streamer/api/hls-token?channel_id=${channelId}`, { method: "POST" });
  const data = await res.json();

  // Build HLS URL (with token if needed)
  const hlsUrl = `/streamer/stream/${channelId}/index.m3u8?token=${data.token}`;

  // Load into Video.js
  player.src({
    src: hlsUrl,
    type: "application/x-mpegURL"
  });

  player.play();
}