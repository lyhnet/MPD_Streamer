const video = document.getElementById('video');
const player = new shaka.Player(video);
const controls = player.getUi().getControls();
player.addEventListener('error', (e) => console.error('Shaka error', e.detail));

// Overlay buttons
const selector = document.getElementById('channel-selector');
const toggleBtn = document.getElementById('toggle-channels');


// Toggle channel selector
toggleBtn.addEventListener('click', () => {
  selector.style.display = selector.style.display === 'none' ? 'block' : 'none';
});

// Load channel on button click
selector.querySelectorAll('button').forEach(btn => {
  btn.addEventListener('click', () => {
    const url = btn.dataset.url;
    player.load(url)
      .then(() => console.log('Loaded', url))
      .catch(err => console.error(err));
    selector.style.display = 'none';
  });
});

// ---------- Cast setup ----------
controls.addEventListener('caststatuschanged', (event) => {
  console.log('New cast status:', event['newStatus']);
});
