(function () {
  const byId = Object.fromEntries(SPECIES.map((s) => [s.id, s]));

  function fallbackImage(id) {
    return `assets/classes/${id}.jpg`;
  }

  function renderSidebar(filterText) {
    const list = document.getElementById("species-list");
    list.innerHTML = "";
    const q = (filterText || "").trim().toLowerCase();

    CATEGORIES.forEach((cat) => {
      const items = SPECIES.filter(
        (s) =>
          s.category === cat &&
          (!q || s.label.toLowerCase().includes(q) || s.id.toLowerCase().includes(q))
      );
      if (!items.length) return;

      const group = document.createElement("div");
      group.className = "category-group";

      const title = document.createElement("div");
      title.className = "category-title";
      title.textContent = cat;
      group.appendChild(title);

      items.forEach((s) => {
        const btn = document.createElement("button");
        btn.className = "species-item" + (s.hasProfile ? " has-profile" : "");
        btn.dataset.id = s.id;
        btn.innerHTML = `
          <span class="name"><span class="dot"></span>${s.label}</span>
          <span class="count">${s.count.toLocaleString()}</span>
        `;
        btn.addEventListener("click", () => selectSpecies(s.id));
        group.appendChild(btn);
      });

      list.appendChild(group);
    });
  }

  function setActive(id) {
    document.querySelectorAll(".species-item").forEach((el) => {
      el.classList.toggle("active", el.dataset.id === id);
    });
  }

  function taxoRow(dtText, ddHtml) {
    return `<dt>${dtText}</dt><dd>${ddHtml}</dd>`;
  }

  function renderDetail(s) {
    const detail = document.getElementById("detail");

    if (!s.hasProfile) {
      detail.innerHTML = `
        <div class="detail-header">
          <h2>${s.label}</h2>
          <p class="meta-line"><span class="badge">${s.category}</span><span class="sep">·</span>${s.count.toLocaleString()} labeled examples in Baseline</p>
        </div>
        <div class="pending-banner">
          Full profile (3-image gallery + WoRMS taxonomy) not built yet for this class — showing the single reference crop and visual note from the original guide.
        </div>
        <div class="gallery" style="grid-template-columns: 1fr;max-width:260px;">
          <figure class="shot">
            <img src="${fallbackImage(s.id)}" alt="${s.label} example crop" onerror="this.style.background='#d9dbdd';this.removeAttribute('src');">
            <figcaption>Reference example crop</figcaption>
          </figure>
        </div>
        <p class="note-line">${s.note}</p>
      `;
      return;
    }

    const t = s.taxonomy;
    const similar = s.similar ? byId[s.similar.id] : null;
    const similarImg = similar ? (similar.hasProfile ? similar.images.good : fallbackImage(similar.id)) : null;

    detail.innerHTML = `
      <div class="detail-header">
        <h2>${s.label} <span class="sci-name">— ${t.name}</span></h2>
        <p class="meta-line">
          <span class="badge">${s.category}</span><span class="sep">·</span>${s.count.toLocaleString()} labeled examples in Baseline
        </p>
      </div>

      <dl class="taxo-box">
        ${taxoRow("Rank", t.rank)}
        ${taxoRow("Name", `${t.name}${t.authority ? " " + t.authority : ""}`)}
        ${taxoRow("Classification", [t.phylum, t.className].filter(Boolean).join(" › "))}
        ${taxoRow("Environment", t.environment)}
        ${taxoRow("Also known as", t.vernacular)}
        ${taxoRow("WoRMS AphiaID", `<a href="${s.wormsUrl}" target="_blank" rel="noopener">${t.aphiaId} ↗</a>`)}
      </dl>

      <div class="gallery">
        <figure class="shot">
          <img src="${s.images.good}" alt="${s.label} reference example">
          <figcaption>Reference example<span class="sub">a clean, representative crop</span></figcaption>
        </figure>
        <figure class="shot">
          <img src="${s.images.features}" alt="${s.label} labeled features">
          <figcaption>Key features<span class="sub">distinguishing structures called out</span></figcaption>
        </figure>
        <figure class="shot similar">
          <img src="${s.images.similar}" alt="${similar ? similar.label : "similar species"} example">
          <figcaption>Commonly confused with<span class="sub"><a href="#" data-goto="${s.similar.id}">${similar ? similar.label : s.similar.id}</a></span></figcaption>
        </figure>
      </div>

      <p class="description">${s.description}</p>

      ${s.similar ? `<div class="similar-note">⚠️ Commonly confused with <a href="#" data-goto="${s.similar.id}">${similar ? similar.label : s.similar.id}</a> — ${s.similar.reason}</div>` : ""}

      <p class="source-line">Taxonomy and AphiaID via <a href="${s.wormsUrl}" target="_blank" rel="noopener">WoRMS</a>. Descriptive text written from general planktonic biology (WoRMS records are taxonomic, not prose descriptions).</p>
    `;

    detail.querySelectorAll("[data-goto]").forEach((el) => {
      el.addEventListener("click", (e) => {
        e.preventDefault();
        selectSpecies(el.dataset.goto);
      });
    });
  }

  function selectSpecies(id) {
    const s = byId[id];
    if (!s) return;
    renderDetail(s);
    setActive(id);
    history.replaceState(null, "", "#" + id);
    document.getElementById("detail").scrollTop = 0;
    window.scrollTo({ top: 0, behavior: "instant" in window ? "instant" : "auto" });
  }

  document.getElementById("search").addEventListener("input", (e) => {
    renderSidebar(e.target.value);
    const activeId = document.querySelector(".species-item.active");
    if (activeId) setActive(activeId.dataset.id);
  });

  renderSidebar("");
  const startId = (location.hash || "").replace("#", "") || "copepod";
  selectSpecies(byId[startId] ? startId : "copepod");
})();
