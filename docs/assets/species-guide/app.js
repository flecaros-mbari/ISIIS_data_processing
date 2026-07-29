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

  function renderWelcome() {
    const detail = document.getElementById("detail");
    const totalCount = SPECIES.reduce((sum, s) => sum + s.count, 0);
    const profileCount = SPECIES.filter((s) => s.hasProfile).length;

    const categoryRows = CATEGORIES.map((cat) => {
      const items = SPECIES.filter((s) => s.category === cat);
      const catTotal = items.reduce((sum, s) => sum + s.count, 0);
      const catProfiles = items.filter((s) => s.hasProfile).length;
      return `<tr>
        <td>${cat}</td>
        <td>${items.length}</td>
        <td>${catTotal.toLocaleString()}</td>
        <td>${catProfiles ? `${catProfiles} / ${items.length}` : "—"}</td>
      </tr>`;
    }).join("");

    detail.innerHTML = `
      <div class="detail-header">
        <h2>ISIIS Labeling Guide</h2>
        <p class="meta-line">${SPECIES.length} classes · ${totalCount.toLocaleString()} labeled examples in the <code>Baseline</code> dataset</p>
      </div>

      <p class="description">This guide documents the class taxonomy used to label regions of interest (ROIs) in
      imagery from the ISIIS (In Situ Ichthyoplankton Imaging System) instrument. Every thumbnail is an actual
      crop from the <code>Baseline</code> labeled dataset
      (<code>/mnt/CFElab/Data_analysis/ISIIS/AI/Baseline</code>), so use it as a visual reference when deciding
      which class an ROI belongs to.</p>

      <p class="description">Descriptions are based on visual appearance in the shadowgraph imagery, not a formal
      taxonomic identification — when a crop is ambiguous between two similar-looking classes, open that class's
      page and check its "commonly confused with" note, or flag it for a second opinion.</p>

      <dl class="taxo-box" style="grid-template-columns: 1fr;">
        <dt>Where labeling happens</dt>
        <dd>ROIs are labeled/verified in <a href="http://mantis.shore.mbari.org" target="_blank" rel="noopener">Tator</a>
        (project <code>902111-CFE</code>). Verified labels are exported to <code>isiis_labels.tsv</code> via
        <a href="https://github.com/flecaros-mbari/ISIIS_data_processing/blob/main/src/labeling/pulling_data.py" target="_blank" rel="noopener">pulling_data.py</a>,
        and a curated snapshot (images, crops, and both YOLO <code>.txt</code> and Pascal VOC <code>.xml</code>
        annotation formats) lives in the <code>Baseline</code> dataset.</dd>
      </dl>

      <p class="description" style="margin-bottom:0.6rem;"><strong>Class breakdown by category</strong> — counts are labeled examples in the
      current Baseline set; the last column shows how many classes in that category have a full profile
      (3-image gallery + WoRMS taxonomy) built out so far.</p>
      <table class="overview-table">
        <thead><tr><th>Category</th><th>Classes</th><th>Examples</th><th>Full profiles</th></tr></thead>
        <tbody>${categoryRows}</tbody>
      </table>

      <p class="description">Pick a class from the list on the right to see its documentation — a reference photo,
      a callout of its key features, a comparison photo of the class it's most often confused with, and (where
      built) its WoRMS taxonomy.</p>
    `;
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
    scrollToTop();
  }

  function showWelcome() {
    renderWelcome();
    setActive(null);
    history.replaceState(null, "", location.pathname + location.search);
    scrollToTop();
  }

  function scrollToTop() {
    document.getElementById("detail").scrollTop = 0;
    window.scrollTo({ top: 0, behavior: "instant" in window ? "instant" : "auto" });
  }

  document.getElementById("search").addEventListener("input", (e) => {
    renderSidebar(e.target.value);
    const activeId = document.querySelector(".species-item.active");
    if (activeId) setActive(activeId.dataset.id);
  });

  const titleLink = document.getElementById("site-title");
  if (titleLink) titleLink.addEventListener("click", (e) => { e.preventDefault(); showWelcome(); });

  renderSidebar("");
  const startId = (location.hash || "").replace("#", "");
  if (startId && byId[startId]) {
    selectSpecies(startId);
  } else {
    showWelcome();
  }
})();
