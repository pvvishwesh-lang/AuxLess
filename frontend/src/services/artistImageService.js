// Uses iTunes Search API — FREE, no API key, already in your pipeline!
const cache = {};

export const getArtistImage = async (artistName) => {
  if (!artistName || artistName === 'Unknown') return null;

  // return cached result immediately
  if (cache[artistName] !== undefined) return cache[artistName];

  try {
    const q   = encodeURIComponent(artistName);
    const res = await fetch(
      `https://itunes.apple.com/search?term=${q}&entity=musicArtist&limit=1`
    );
    const data = await res.json();

    if (data.results?.length > 0) {
      // iTunes gives 30x30 — replace with 200x200
      const img = data.results[0].artworkUrl30
        ?.replace('30x30', '200x200');

      cache[artistName] = img || null;
      return img || null;
    }
  } catch (e) {}

  cache[artistName] = null;
  return null;
};

// Fetch multiple at once
export const prefetchArtistImages = async (artistNames) => {
  await Promise.all(artistNames.map(a => getArtistImage(a)));
};