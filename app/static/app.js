const dealsBtn = document.getElementById('findDealsBtn');
const searchBtn = document.getElementById('searchBtn');

function itemHtml(deal) {
  const offer = deal.best_offer;
  return `<div class="result-item">
    <div><strong>${deal.requested_item}</strong></div>
    <div class="price">€${offer.price.toFixed(2)} - ${offer.supermarket}</div>
    <div class="muted">Matched: ${offer.description} (${offer.category})</div>
  </div>`;
}

dealsBtn.addEventListener('click', async () => {
  const raw = document.getElementById('basketItems').value;
  const items = raw.split('\n').map(x => x.trim()).filter(Boolean);
  const out = document.getElementById('dealsResult');
  if (!items.length) {
    out.innerHTML = '<p class="muted">Please add at least one item.</p>';
    return;
  }

  const res = await fetch('/api/best-deals', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({items})
  });
  const data = await res.json();

  if (!data.deals.length) {
    out.innerHTML = '<p class="muted">No matches found.</p>';
    return;
  }

  out.innerHTML = data.deals.map(itemHtml).join('') + `<p><strong>Total: €${data.total.toFixed(2)}</strong></p>`;
});

searchBtn.addEventListener('click', async () => {
  const query = document.getElementById('searchInput').value.trim();
  const out = document.getElementById('searchResult');
  if (!query) {
    out.innerHTML = '';
    return;
  }

  const res = await fetch(`/api/search?query=${encodeURIComponent(query)}`);
  const data = await res.json();
  out.innerHTML = data.results
    .slice(0, 8)
    .map(r => `<div class="result-item"><strong>${r.description}</strong> - ${r.supermarket} - <span class="price">€${r.price.toFixed(2)}</span> <span class="muted">(score ${r.score})</span></div>`)
    .join('');
});
