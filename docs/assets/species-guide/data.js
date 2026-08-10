// Data for the interactive ISIIS labeling guide.
// Each entry corresponds to one label class used in the `Baseline` dataset.
//
// `hasProfile: true` classes have a full 3-image profile (good / features / similar)
// plus a written description. The few classes still `hasProfile: false` show their
// single reference image and short visual note, marked "profile coming soon".
//
// `isBiological: true` classes additionally carry a `taxonomy` block and `wormsUrl`
// sourced from WoRMS (https://www.marinespecies.org/aphia.php?p=taxlist) via the linked
// AphiaID. WoRMS records are taxonomic (name, rank, classification, environment) rather
// than prose descriptions, so the `description` text is written by hand from general
// planktonic biology, not copied from WoRMS. `isBiological: false` classes are
// non-taxonomic categories (imaging artifacts, marine snow, shed exoskeletons) — there is
// no WoRMS entry for them, so they carry a manually written description only.

const CATEGORIES = [
  "Radiolarians & rhizarians",
  "Diatoms",
  "Gelatinous zooplankton",
  "Crustaceans & other animals",
  "Colonial algae",
  "Marine snow, aggregates & fecal material",
  "Out-of-focus / blurred particles",
  "Non-biological / imaging artifacts",
];

const SPECIES = [
  // ---- Radiolarians & rhizarians ----
  {
    id: "acantharia", label: "Acantharia", category: "Radiolarians & rhizarians", count: 9, hasProfile: true, isBiological: true,
    note: "Small dark central capsule with a few long, straight, radiating spicules.",
    description: "Acantharians are single-celled marine protists (phylum Radiozoa) related to radiolarians, built around a skeleton of radiating strontium sulfate spicules rather than the silica used by true radiolarians and diatoms. Because strontium sulfate dissolves quickly after death, an intact skeleton like the one below is a sign of a fresh, recently living capture. In shadowgraph they show a small dark central capsule with a handful of long, straight spicules radiating outward.",
    taxonomy: { rank: "Class", name: "Acantharia", authority: null, phylum: "Radiozoa", className: "Acantharia", environment: "Marine", vernacular: "acantharians" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=586732",
    images: { good: "assets/species/acantharia/good.jpg", features: "assets/species/acantharia/features.jpg", similar: "assets/species/acantharia/similar.jpg" },
    similar: { id: "diatom chain spines", reason: "Both show a dark central body with several long, thin spicules/spines radiating outward. Diatom chain spines has a longer body compared with the sphere body of the acantharia." },
  },
  {
    id: "phaeodarian_A", label: "Phaeodarian A", category: "Radiolarians & rhizarians", count: 36, hasProfile: true, isBiological: true,
    note: "Solid dark oval body with a fine textured/granular interior, no long spines.",
    description: "Phaeodarians are single-celled marine Rhizaria (subclass Phaeodaria) that build intricate skeletons of amorphous silica, typically enclosing a mass of dark pigmented granules called the phaeodium near the cell center — the dark body seen in every crop here. Skeleton shape varies enormously between genera (solid, latticed, spined, or radially striated), which is why this dataset splits phaeodarians into six informal visual morphotypes (A–F) rather than one class. Morphotype A is the solid, spineless end of that spectrum: a dense oval body with a finely granular or cross-hatched interior texture and no spines or lattice visible.",
    taxonomy: { rank: "Subclass", name: "Phaeodaria", authority: "Haeckel, 1879", phylum: "Cercozoa", className: "Thecofilosea (subclass Phaeodaria)", environment: "Marine", vernacular: "phaeodarians" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=345868",
    images: { good: "assets/species/phaeodarian_A/good.jpg", features: "assets/species/phaeodarian_A/features.jpg", similar: "assets/species/phaeodarian_A/similar.jpg" },
    similar: { id: "phaeodarian_D", reason: "Both morphotypes lack protruding spines and show fine internal texture rather than a smooth or lattice surface — A's texture is a granular cross-hatch, D's is radial sunburst striations. Compare against all six phaeodarian morphotypes before deciding." },
  },
  {
    id: "phaeodarian_B", label: "Phaeodarian B", category: "Radiolarians & rhizarians", count: 20, hasProfile: true, isBiological: true,
    note: "Dark irregular star-shaped mass with short jagged spines all around.",
    description: "See `phaeodarian_A` for background on phaeodarians and why this dataset splits them into six visual morphotypes. Morphotype B shows a dense, irregular, star-shaped central mass with short jagged spines projecting all around the body — a spinier, less symmetric silhouette than the evenly-spaced long spicules of acantharia or phaeodarian E.",
    taxonomy: { rank: "Subclass", name: "Phaeodaria", authority: "Haeckel, 1879", phylum: "Cercozoa", className: "Thecofilosea (subclass Phaeodaria)", environment: "Marine", vernacular: "phaeodarians" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=345868",
    images: { good: "assets/species/phaeodarian_B/good.jpg", features: "assets/species/phaeodarian_B/features.jpg", similar: "assets/species/phaeodarian_B/similar.jpg" },
    similar: { id: "phaeodarian_F", reason: "Both morphotypes show a dark central mass with numerous short, jagged spines rather than a few long ones. Phaeodarian F's spines are finer and give a fuzzier, more starburst-like edge than B's coarser jagged spines." },
  },
  {
    id: "phaeodarian_C", label: "Phaeodarian C", category: "Radiolarians & rhizarians", count: 8, hasProfile: true, isBiological: true,
    note: "Round lattice/honeycomb \"shell\" pattern surrounding a small dark central body.",
    description: "See `phaeodarian_A` for background on phaeodarians and why this dataset splits them into six visual morphotypes. Morphotype C is built around a round, lattice- or honeycomb-patterned silica shell surrounding a small dark central body — the perforated lattice shell is the distinguishing feature, rather than spines or striations.",
    taxonomy: { rank: "Subclass", name: "Phaeodaria", authority: "Haeckel, 1879", phylum: "Cercozoa", className: "Thecofilosea (subclass Phaeodaria)", environment: "Marine", vernacular: "phaeodarians" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=345868",
    images: { good: "assets/species/phaeodarian_C/good.jpg", features: "assets/species/phaeodarian_C/features.jpg", similar: "assets/species/phaeodarian_C/similar.jpg" },
    similar: { id: "phaeodarian_D", reason: "Both morphotypes are round with a patterned shell/surface and no protruding spines. C's pattern is a honeycomb lattice; D's is fine radial sunburst striations." },
  },
  {
    id: "phaeodarian_D", label: "Phaeodarian D", category: "Radiolarians & rhizarians", count: 4, hasProfile: true, isBiological: true,
    note: "Round body with fine radial striations (sunburst pattern) around a solid dark center, no protruding spines.",
    description: "See `phaeodarian_A` for background on phaeodarians and why this dataset splits them into six visual morphotypes. Morphotype D is a round body with fine radial striations fanning out from a solid dark center like a sunburst, enclosed by a thin outer membrane — no spines protrude past the body outline, unlike most other morphotypes.",
    taxonomy: { rank: "Subclass", name: "Phaeodaria", authority: "Haeckel, 1879", phylum: "Cercozoa", className: "Thecofilosea (subclass Phaeodaria)", environment: "Marine", vernacular: "phaeodarians" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=345868",
    images: { good: "assets/species/phaeodarian_D/good.jpg", features: "assets/species/phaeodarian_D/features.jpg", similar: "assets/species/phaeodarian_D/similar.jpg" },
    similar: { id: "phaeodarian_A", reason: "Both morphotypes lack protruding spines and show fine internal texture rather than a smooth surface — D's texture is radial sunburst striations, A's is a granular cross-hatch. Compare against all six phaeodarian morphotypes before deciding." },
  },
  {
    id: "phaeodarian_E", label: "Phaeodarian E", category: "Radiolarians & rhizarians", count: 4, hasProfile: true, isBiological: true,
    note: "Dark center with a moderate number of long, thin, evenly spaced spines — sparser than acantharia.",
    description: "See `phaeodarian_A` for background on phaeodarians and why this dataset splits them into six visual morphotypes. Morphotype E has a dark central mass with a moderate number of long, thin, evenly spaced spines radiating outward — visually the closest of the six to acantharia, but with fewer and shorter spines.",
    taxonomy: { rank: "Subclass", name: "Phaeodaria", authority: "Haeckel, 1879", phylum: "Cercozoa", className: "Thecofilosea (subclass Phaeodaria)", environment: "Marine", vernacular: "phaeodarians" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=345868",
    images: { good: "assets/species/phaeodarian_E/good.jpg", features: "assets/species/phaeodarian_E/features.jpg", similar: "assets/species/phaeodarian_E/similar.jpg" },
    similar: { id: "acantharia", reason: "Both show a dark central body with several long, thin spicules/spines radiating outward. Acantharia's spicules are longer, straighter, and sparser than phaeodarian E's more numerous, evenly-spaced spines." },
  },
  {
    id: "phaeodarian_F", label: "Phaeodarian F", category: "Radiolarians & rhizarians", count: 5, hasProfile: true, isBiological: true,
    note: "Small dark center with many short, fine spines giving a fuzzy/starburst edge.",
    description: "See `phaeodarian_A` for background on phaeodarians and why this dataset splits them into six visual morphotypes. Morphotype F has a small dark center surrounded by many short, fine spines, giving the whole body a fuzzy, starburst-like edge rather than a few discrete, countable spines.",
    taxonomy: { rank: "Subclass", name: "Phaeodaria", authority: "Haeckel, 1879", phylum: "Cercozoa", className: "Thecofilosea (subclass Phaeodaria)", environment: "Marine", vernacular: "phaeodarians" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=345868",
    images: { good: "assets/species/phaeodarian_F/good.jpg", features: "assets/species/phaeodarian_F/features.jpg", similar: "assets/species/phaeodarian_F/similar.jpg" },
    similar: { id: "phaeodarian_B", reason: "Both morphotypes show a dark central mass with numerous short, jagged spines rather than a few long ones. Phaeodarian F's spines are finer and give a fuzzier, more starburst-like edge than B's coarser jagged spines." },
  },

  // ---- Diatoms ----
  {
    id: "centric_diatom", label: "Centric diatom", category: "Diatoms", count: 27, hasProfile: true, isBiological: true,
    note: "Single small, solid, roundish/barrel-shaped dark cell, no chain.",
    description: "Centric diatoms are single-celled algae with radially symmetric, often drum- or disc-shaped silica cell walls (frustules) — unlike the elongated, bilaterally symmetric pennate diatoms that form the chain classes in this dataset. They can occur singly or in chains, but this class is reserved for single, unattached cells: a small, solid, roundish dark cell with a visible cell-wall rim and no chain attached.",
    taxonomy: { rank: "Subclass", name: "Coscinodiscophycidae", authority: "Round & Crawford, 1990", phylum: "Ochrophyta", className: "Coscinodiscophyceae (subclass Coscinodiscophycidae)", environment: "Marine", vernacular: "centric diatoms" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=148971",
    images: { good: "assets/species/centric_diatom/good.jpg", features: "assets/species/centric_diatom/features.jpg", similar: "assets/species/centric_diatom/similar.jpg" },
    similar: { id: "bubble", reason: "Both are small, solid, dark, roughly circular shapes. Check for the diatom's visible cell wall (frustule) rim and slightly irregular internal texture, versus the bubble's perfectly circular, featureless, uniformly black disc." },
  },
  {
    id: "diatom_chain_straight", label: "Diatom chain (straight)", category: "Diatoms", count: 725, hasProfile: true, isBiological: true,
    note: "Row of small dark cells strung together in a straight or gently curved line.",
    description: "Chain-forming diatoms are single-celled algae (class Bacillariophyceae) that remain attached after cell division, forming a row of cells end-to-end. Chain shape and ornamentation vary by genus and are split into three separate visual classes here rather than one 'diatom chain' label. This class covers chains that stay in a straight or gently curved row of individual cells, without spiraling into a coil or bearing long spines.",
    taxonomy: { rank: "Class", name: "Bacillariophyceae", authority: "Haeckel, 1878", phylum: "Heterokontophyta (Ochrophyta)", className: "Bacillariophyceae", environment: "Marine, brackish, fresh, terrestrial", vernacular: "diatoms" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=148899",
    images: { good: "assets/species/diatom_chain_straight/good.jpg", features: "assets/species/diatom_chain_straight/features.jpg", similar: "assets/species/diatom_chain_straight/similar.jpg" },
    similar: { id: "diatom_chain_spiral", reason: "Both are chains of diatom cells joined end-to-end. The straight chain stays in a loose row or gentle curve, while the spiral chain coils tightly into a helix — a spectrum of the same growth form rather than two unrelated shapes." },
  },
  {
    id: "diatom_chain_spiral", label: "Diatom chain (spiral)", category: "Diatoms", count: 2262, hasProfile: true, isBiological: true,
    note: "Chain of cells curled into a tight spiral/coil rather than a line.",
    description: "See `diatom_chain_straight` for background on chain-forming diatoms. This class covers chains that coil into a tight spiral or helix rather than staying in a line — often visible sitting inside a larger translucent gelatinous sheath (mucilage envelope) that the colony secretes around itself.",
    taxonomy: { rank: "Class", name: "Bacillariophyceae", authority: "Haeckel, 1878", phylum: "Heterokontophyta (Ochrophyta)", className: "Bacillariophyceae", environment: "Marine, brackish, fresh, terrestrial", vernacular: "diatoms" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=148899",
    images: { good: "assets/species/diatom_chain_spiral/good.jpg", features: "assets/species/diatom_chain_spiral/features.jpg", similar: "assets/species/diatom_chain_spiral/similar.jpg" },
    similar: { id: "diatom_chain_straight", reason: "Both are chains of diatom cells joined end-to-end. The spiral chain coils tightly into a helix, while the straight chain stays in a loose row or gentle curve — a spectrum of the same growth form rather than two unrelated shapes." },
  },
  {
    id: "diatom_chain_spines", label: "Diatom chain (spines)", category: "Diatoms", count: 999, hasProfile: true, isBiological: true,
    note: "Chain of cells with fine spines/setae radiating outward, giving a fan-like or bristly appearance.",
    description: "See `diatom_chain_straight` for background on chain-forming diatoms. This class covers chains whose cells bear fine spines or setae radiating outward from the chain axis, giving the colony a fan-like or bristly silhouette — a growth form common in genera like Chaetoceros, where the setae help the colony resist sinking and grazing.",
    taxonomy: { rank: "Class", name: "Bacillariophyceae", authority: "Haeckel, 1878", phylum: "Heterokontophyta (Ochrophyta)", className: "Bacillariophyceae", environment: "Marine, brackish, fresh, terrestrial", vernacular: "diatoms" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=148899",
    images: { good: "assets/species/diatom_chain_spines/good.jpg", features: "assets/species/diatom_chain_spines/features.jpg", similar: "assets/species/diatom_chain_spines/similar.jpg" },
    similar: { id: "diatom_chain_straight", reason: "Both are chains of diatom cells joined end-to-end. This class's cells bear fine radiating spines/setae giving a bristly fan shape, while the straight chain's cells have no spines at all." },
  },

  // ---- Gelatinous zooplankton ----
  {
    id: "larvacean", label: "Larvacean", category: "Gelatinous zooplankton", count: 355, hasProfile: true, isBiological: true,
    note: "Dark, S-curved or comma-shaped elongated body, tadpole-like.",
    description: "Larvaceans are small, tadpole-shaped pelagic tunicates that build and continuously discard an external gelatinous mucous \"house\" used to filter extremely fine food particles from the water. The house itself is delicate and rarely visible in shadowgraph imagery, but the animal's dark, S-curved or comma-shaped trunk and long beating tail are distinctive.",
    taxonomy: { rank: "Class", name: "Appendicularia", authority: null, phylum: "Chordata", className: "Appendicularia", environment: "Marine, brackish", vernacular: "larvaceans" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=146421",
    images: { good: "assets/species/larvacean/good.jpg", features: "assets/species/larvacean/features.jpg", similar: "assets/species/larvacean/similar.jpg" },
    similar: { id: "copepod", reason: "Both are small, dark, comma-shaped bodies of similar size. Look for the larvacean's smooth S-curved tail and lack of antennae, versus the copepod's pair of long antennae and fine trailing tail setae." },
  },
  {
    id: "salp", label: "Salp", category: "Gelatinous zooplankton", count: 5, hasProfile: true, isBiological: true,
    note: "Translucent barrel/oval body with visible ring-like muscle bands and a small dark internal mass; often a thin trailing thread.",
    description: "Salps are barrel-shaped, gelatinous pelagic tunicates that swim by jet propulsion, pumping water through the body wall with each muscular contraction. They often form long chains by asexual budding, though isolated solitary individuals (as labeled here) are common in ISIIS imagery. In shadowgraph, a salp shows a translucent oval/barrel outline with clear ring-like muscle bands encircling the body and a small dark internal mass (the visceral \"nucleus\"), often with a thin trailing thread of mucus or fecal material.",
    taxonomy: { rank: "Order", name: "Salpida", authority: "Forbes, 1853", aphiaId: 137214, phylum: "Chordata", className: "Thaliacea", environment: "Marine (pelagic tunicates)", vernacular: "salps" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=137214",
    images: { good: "assets/species/salp/good.jpg", features: "assets/species/salp/features.jpg", similar: "assets/species/salp/similar.jpg" },
    similar: { id: "siphonophore", reason: "Both appear as an oval/barrel translucent body with internal banding or canals. Check for the salp's ring-like muscle bands and lack of a trailing tentacle, versus the siphonophore's radiating canal pattern and long trailing strand." },
  },
  {
    id: "siphonophore", label: "Siphonophore", category: "Gelatinous zooplankton", count: 38, hasProfile: true, isBiological: true,
    note: "Similar oval/barrel float with striations, usually paired with a long thin trailing tentacle/strand extending well outside the body.",
    description: "Siphonophores are colonial hydrozoans — a chain of specialized, genetically identical zooids (each doing one job: floating, swimming, feeding, or reproducing) that function together as a single organism. The label in this dataset mainly corresponds to physonect siphonophores viewed by their gas-filled pneumatophore or a nectophore (swimming bell): a translucent, fan- or bell-shaped structure with fine radiating canals and scattered dark pigment spots, usually trailing one or more long, thin tentacles extending well outside the body used to capture prey.",
    taxonomy: { rank: "Order", name: "Siphonophorae", authority: "Eschscholtz, 1829", aphiaId: 1371, phylum: "Cnidaria", className: "Hydrozoa", environment: "Primarily marine/pelagic", vernacular: "siphonophores" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=1371",
    images: { good: "assets/species/siphonophore/good.jpg", features: "assets/species/siphonophore/features.jpg", similar: "assets/species/siphonophore/similar.jpg" },
    similar: { id: "salp", reason: "Both appear as an oval/barrel translucent body with internal banding or canals. Check for the siphonophore's radiating canal pattern, pigment spots, and long trailing tentacle, versus the salp's evenly spaced ring-like muscle bands and lack of a tentacle." },
  },
  {
    id: "jelly", label: "Jelly", category: "Gelatinous zooplankton", count: 15, hasProfile: true, isBiological: true,
    note: "Thin, irregular membranous outline with fine trailing tentacle-like strands; no compact solid body.",
    description: "The `jelly` class covers true jellyfish (class Scyphozoa) and other soft-bodied medusae captured too incompletely, or at too oblique an angle, to place in a more specific class. In shadowgraph they show a thin, irregular, membranous outline with fine trailing tentacle-like strands and no compact solid body — unlike the well-defined float of a siphonophore.",
    taxonomy: { rank: "Class", name: "Scyphozoa", authority: "Goette, 1887", phylum: "Cnidaria", className: "Scyphozoa", environment: "Marine, brackish", vernacular: "jellyfish" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=135220",
    images: { good: "assets/species/jelly/good.jpg", features: "assets/species/jelly/features.jpg", similar: "assets/species/jelly/similar.jpg" },
    similar: { id: "siphonophore", reason: "Both are translucent gelatinous animals with fine trailing tentacle-like strands. The jelly has no compact solid body or defined float, while the siphonophore has a distinct fan-shaped nectophore with radiating canals." },
  },
  {
    id: "ctenophore", label: "Ctenophore", category: "Gelatinous zooplankton", count: 7, hasProfile: true, isBiological: true,
    note: "Body showing parallel curved comb-row ridges/banding.",
    description: "Ctenophores (comb jellies) are gelatinous, predatory pelagic animals related only distantly to true jellyfish, propelled by eight rows of fused cilia (comb rows) that beat in coordinated waves and diffract light into a shimmering, banded pattern — visible in shadowgraph as parallel curved ridges running down the body.",
    taxonomy: { rank: "Phylum", name: "Ctenophora", authority: "Eschscholtz, 1829", phylum: "Ctenophora", className: null, environment: "Marine, brackish", vernacular: "comb jellies" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=1248",
    images: { good: "assets/species/ctenophore/good.jpg", features: "assets/species/ctenophore/features.jpg", similar: "assets/species/ctenophore/similar.jpg" },
    similar: { id: "salp", reason: "Both show a banded or ridged pattern on a translucent body. The ctenophore's comb rows are curved parallel ridges with no barrel-shaped outline, while the salp has evenly spaced ring-like muscle bands around a clear barrel/oval body." },
  },

  // ---- Crustaceans & other animals ----
  {
    id: "copepod", label: "Copepod", category: "Crustaceans & other animals", count: 1358, hasProfile: true, isBiological: true,
    note: "Small dark comma/teardrop-shaped body with thin trailing antennae/tail setae.",
    description: "Copepods are small crustaceans and among the most numerous multicellular animals on Earth, forming a dominant part of the mesozooplankton in every ocean. Most specimens labeled in this set are calanoid copepods, recognizable in shadowgraph imagery by a dark, comma- or teardrop-shaped cephalothorax (head + thorax), a pair of long antennae projecting forward and outward, and a narrower segmented abdomen trailing into fine caudal setae (\"tail\" bristles) used for sensing and swimming.",
    taxonomy: { rank: "Class", name: "Copepoda", authority: "Milne Edwards, 1840", aphiaId: 1080, phylum: "Arthropoda", className: "Copepoda (subphylum Crustacea)", environment: "Marine, brackish, fresh, terrestrial", vernacular: "copepods" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=1080",
    images: { good: "assets/species/copepod/good.jpg", features: "assets/species/copepod/features.jpg", similar: "assets/species/copepod/similar.jpg" },
    similar: { id: "molt", reason: "A shed copepod exoskeleton keeps the exact same silhouette — antennae, body outline, tail setae — but reads as translucent/empty in shadowgraph, lacking the solid dark body mass of a living animal." },
  },
  {
    id: "chaetognath", label: "Chaetognath", category: "Crustaceans & other animals", count: 42, hasProfile: true, isBiological: true,
    note: "Straight, elongated, torpedo-shaped translucent body tapering to a point (arrow worm).",
    description: "Chaetognaths, or \"arrow worms\", are voracious planktonic predators found throughout the world's oceans. They have a straight, transparent, torpedo-shaped body: a distinct head at one end, a long trunk showing longitudinal muscle bands (visible in shadowgraph as fine striations running the length of the body, with the gut often visible as a darker central line), and a paddle-like tail fin with small hooked grasping spines near the head used to seize prey.",
    taxonomy: { rank: "Phylum", name: "Chaetognatha", authority: null, aphiaId: 2081, phylum: "Chaetognatha", className: "Sagittoidea", environment: "Marine only", vernacular: "arrow worms" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=2081",
    images: { good: "assets/species/chaetognath/good.jpg", features: "assets/species/chaetognath/features.jpg", similar: "assets/species/chaetognath/similar.jpg" },
    similar: { id: "long_fecal_pellet", reason: "Both are straight, elongated, dark shapes of similar size. Look for the chaetognath's tapered head, tail fin, and internal muscle striations, versus the fecal pellet's uniform width and otherwise featureless interior." },
  },
  {
    id: "worm", label: "Worm", category: "Crustaceans & other animals", count: 12, hasProfile: true, isBiological: true,
    note: "Elongated segmented body with paired bristle-like parapodia along both sides.",
    description: "The `worm` class covers polychaetes (bristle worms) captured in the imagery — segmented marine annelids with a pair of fleshy, bristle-bearing parapodia on every body segment, used for crawling and swimming. In shadowgraph they show an elongated segmented body with rows of paired bristle bundles along both sides.",
    taxonomy: { rank: "Class", name: "Polychaeta", authority: "Grube, 1850", phylum: "Annelida", className: "Polychaeta", environment: "Marine, brackish, fresh, terrestrial", vernacular: "bristle worms" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=883",
    images: { good: "assets/species/worm/good.jpg", features: "assets/species/worm/features.jpg", similar: "assets/species/worm/similar.jpg" },
    similar: { id: "diatom_chain_spines", reason: "Both show a central elongated axis with fine lateral projections. The worm's paired parapodia are thicker and more solid than a spiny diatom chain's fine bristles, and its central body segment is much wider." },
  },
  {
    id: "molt", label: "Molt", category: "Crustaceans & other animals", count: 5, hasProfile: true, isBiological: false,
    note: "Empty, translucent, articulated exoskeleton outline (legs/antennae visible) with no solid dark body mass — a shed carapace, not a living animal.",
    description: "A molt is the shed exoskeleton (exuvia) left behind when a crustacean — almost always a copepod in this dataset — grows and casts off its old cuticle. It keeps the exact silhouette of the animal it came from (antennae, body outline, tail setae) but reads as an empty, translucent, articulated husk in shadowgraph imagery, with no solid dark body mass inside. It isn't a distinct organism or taxon, which is why there's no WoRMS entry for it — it's included as its own class because empty molts are easy to mistake for a living animal.",
    images: { good: "assets/species/molt/good.jpg", features: "assets/species/molt/features.jpg", similar: "assets/species/molt/similar.jpg" },
    similar: { id: "copepod", reason: "A molt keeps the exact silhouette of the copepod it came from — antennae, body outline, tail setae — but reads as an empty, translucent husk, lacking the solid dark body mass of the living animal." },
  },
  {
    id: "shrimp", label: "Shrimp", category: "Crustaceans & other animals", count: 7, hasProfile: true, isBiological: true,
    note: "Visible jointed legs/antennae and a segmented body/tail fan.",
    description: "The `shrimp` class covers decapod crustaceans (shrimp, and any similar-looking decapod larvae or juveniles) captured in the imagery. They're recognizable by long jointed antennae, visible jointed walking/swimming legs, and a segmented body ending in a fan-shaped tail (the uropods and telson).",
    taxonomy: { rank: "Order", name: "Decapoda", authority: "Latreille, 1802", phylum: "Arthropoda", className: "Malacostraca (order Decapoda)", environment: "Marine, brackish, fresh, terrestrial", vernacular: "decapods" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=1130",
    images: { good: "assets/species/shrimp/good.jpg", features: "assets/species/shrimp/features.jpg", similar: "assets/species/shrimp/similar.jpg" },
    similar: { id: "molt", reason: "Both show jointed legs and antennae in a similar overall body plan. A shrimp is a solid, opaque living animal, while a molt is an empty, translucent shed exoskeleton with no solid body mass." },
  },
  {
    id: "squid", label: "Squid", category: "Crustaceans & other animals", count: 1, hasProfile: true, isBiological: true,
    note: "Large solid dark silhouette with a distinct mantle and fin shape.",
    description: "Squid are fast-swimming cephalopod predators. Only a single example is labeled in the current Baseline set, seen as a large, solid, dark silhouette with a distinct tapered mantle and pointed arms/fins trailing behind — much larger than almost anything else in this taxonomy. (WoRMS flags the order Teuthida itself as taxonomically unresolved pending further study, but it remains the standard name in general use for squids.)",
    taxonomy: { rank: "Order", name: "Teuthida", authority: "Naef, 1916", phylum: "Mollusca", className: "Cephalopoda (order Teuthida)", environment: "Marine", vernacular: "squids" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=11716",
    images: { good: "assets/species/squid/good.jpg", features: "assets/species/squid/features.jpg", similar: "assets/species/squid/similar.jpg" },
    similar: { id: "aggregate_dense", reason: "Both can appear as a large, solid, dark silhouette at first glance. Check for the squid's distinct tapered mantle outline and pointed arms/fins, versus the aggregate's shapeless, uniformly dark blob." },
  },

  // ---- Colonial algae ----
  {
    id: "phaeocystis", label: "Phaeocystis", category: "Colonial algae", count: 2328, hasProfile: true, isBiological: true,
    note: "Fuzzy, irregular dark colonial clump, usually trailing one or two long parallel straight strands.",
    description: "Phaeocystis is a genus of haptophyte algae that alternates between free-living single cells and large gelatinous colonies containing hundreds of cells embedded in a mucilaginous matrix. Colony blooms can be massive and are notorious for producing thick, foul-smelling foam on beaches when they break down. In shadowgraph they appear as a fuzzy, irregular dark colonial clump, usually trailing one or two long, parallel, straight mucilage strands.",
    taxonomy: { rank: "Genus", name: "Phaeocystis", authority: "Lagerheim, 1893", phylum: "Haptophyta", className: "Coccolithophyceae (order Phaeocystales)", environment: "Marine", vernacular: null },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=115088",
    images: { good: "assets/species/phaeocystis/good.jpg", features: "assets/species/phaeocystis/features.jpg", similar: "assets/species/phaeocystis/similar.jpg" },
    similar: { id: "aggregate_string", reason: "Both show a fuzzy, irregular mass trailing one or two long straight strands. Phaeocystis is a distinct living colonial clump with a consistent fuzzy texture, while aggregate_string is detrital marine snow of variable, often more compact, texture." },
  },

  // ---- Marine snow, aggregates & fecal material ----
  {
    id: "aggregate_dense", label: "Aggregate (dense)", category: "Marine snow, aggregates & fecal material", count: 5815, hasProfile: true, isBiological: false,
    note: "Solid, compact, uniformly dark blob with little internal structure — most common class in the set.",
    description: "Aggregates are loose clumps of marine snow — detritus, mucus, dead organisms, and fecal material that stick together and sink through the water column, an important pathway for carbon export to the deep ocean. `aggregate_dense` is the solid, compact end of that spectrum: a uniformly dark blob with almost no internal structure and a defined, in-focus edge — the single most common class in the Baseline set.",
    images: { good: "assets/species/aggregate_dense/good.jpg", features: "assets/species/aggregate_dense/features.jpg", similar: "assets/species/aggregate_dense/similar.jpg" },
    similar: { id: "particle_blur", reason: "Both are solid, roughly round dark masses. aggregate_dense is in focus with a sharp, defined edge, while particle_blur is the same kind of object far enough out of the focal plane that its edges are soft and blurred." },
  },
  {
    id: "aggregate_light", label: "Aggregate (light)", category: "Marine snow, aggregates & fecal material", count: 287, hasProfile: true, isBiological: false,
    note: "Looser, lower-contrast/more translucent version of aggregate_dense — less dense, more diffuse edges.",
    description: "The same detrital marine-snow material as `aggregate_dense`, but at its loosest and most diffuse: a lower-contrast, more translucent clump with ragged, diffuse edges rather than a solid, well-defined outline.",
    images: { good: "assets/species/aggregate_light/good.jpg", features: "assets/species/aggregate_light/features.jpg", similar: "assets/species/aggregate_light/similar.jpg" },
    similar: { id: "aggregate_dense", reason: "Both are marine-snow aggregates. aggregate_light is looser and more diffuse with ragged edges, while aggregate_dense is a solid, compact, uniformly dark blob with a defined edge." },
  },
  {
    id: "aggregate_string", label: "Aggregate (string)", category: "Marine snow, aggregates & fecal material", count: 740, hasProfile: true, isBiological: false,
    note: "Aggregate mass hanging off a single long, thin string/strand.",
    description: "Marine-snow aggregate material (see `aggregate_dense`) that has collected around, or is hanging from, a single long thin string or strand — often a discarded larvacean house strand or a fecal strand that debris has accumulated on.",
    images: { good: "assets/species/aggregate_string/good.jpg", features: "assets/species/aggregate_string/features.jpg", similar: "assets/species/aggregate_string/similar.jpg" },
    similar: { id: "phaeocystis", reason: "Both show a fuzzy, irregular mass trailing one or two long straight strands. aggregate_string is detrital marine snow of variable texture, while phaeocystis is a distinct living colonial clump with a consistent fuzzy texture." },
  },
  {
    id: "bloom", label: "Bloom", category: "Marine snow, aggregates & fecal material", count: 21311, hasProfile: true, isBiological: false,
    note: "Frame scattered with many small round particles/cells rather than one discrete object — most common class overall.",
    description: "`bloom` is a frame-level label, not a single-object one: it's used when the frame is scattered with many small round particles or cells — typically a dense phytoplankton bloom — rather than showing one discrete organism or particle to crop. It's the most common class overall in the Baseline set.",
    images: { good: "assets/species/bloom/good.jpg", features: "assets/species/bloom/features.jpg", similar: "assets/species/bloom/similar.jpg" },
    similar: { id: "density", reason: "Both are frame-wide labels rather than single discrete objects. bloom shows many small distinguishable round particles/cells scattered throughout, while density is a smooth, mottled shading gradient with no distinguishable particles at all." },
  },
  {
    id: "long_fecal_pellet", label: "Fecal pellet (long)", category: "Marine snow, aggregates & fecal material", count: 414, hasProfile: true, isBiological: false,
    note: "Long, slender, solid dark cylindrical pellet, uniform width.",
    description: "Zooplankton fecal pellets are dense, compact packages of undigested material egested after feeding, and are one of the main vehicles for carbon export to the deep ocean. `long_fecal_pellet` is a long, slender, solid dark cylinder of uniform width with blunt or rounded ends and a largely featureless interior.",
    images: { good: "assets/species/long_fecal_pellet/good.jpg", features: "assets/species/long_fecal_pellet/features.jpg", similar: "assets/species/long_fecal_pellet/similar.jpg" },
    similar: { id: "chaetognath", reason: "Both are straight, elongated, dark shapes of similar size. The fecal pellet has a uniform width and featureless interior, while the chaetognath tapers to a head and tail fin and shows internal muscle striations." },
  },
  {
    id: "short_fecal_pellet", label: "Fecal pellet (short)", category: "Marine snow, aggregates & fecal material", count: 11, hasProfile: true, isBiological: false,
    note: "Same as long_fecal_pellet but short/oval rather than elongated.",
    description: "The same solid, dense fecal material as `long_fecal_pellet` (see there for background), but short and oval rather than elongated — a solid, uniformly dark pellet with a sharp, defined edge.",
    images: { good: "assets/species/short_fecal_pellet/good.jpg", features: "assets/species/short_fecal_pellet/features.jpg", similar: "assets/species/short_fecal_pellet/similar.jpg" },
    similar: { id: "long_fecal_pellet", reason: "Both are solid, uniformly dark fecal pellets with a defined edge and featureless interior. short_fecal_pellet is a short oval shape, while long_fecal_pellet is a longer, more slender cylinder." },
  },
  {
    id: "loose_fecal_pellet", label: "Fecal pellet (loose)", category: "Marine snow, aggregates & fecal material", count: 221, hasProfile: true, isBiological: false,
    note: "Irregular, crumbly-edged dark mass — less uniform/compact than a solid pellet.",
    description: "A fecal pellet (see `long_fecal_pellet`) that has started to break apart, or was loosely packed to begin with: an irregular, crumbly-edged dark mass rather than a solid, uniform cylinder.",
    images: { good: "assets/species/loose_fecal_pellet/good.jpg", features: "assets/species/loose_fecal_pellet/features.jpg", similar: "assets/species/loose_fecal_pellet/similar.jpg" },
    similar: { id: "aggregate_light", reason: "Both show a looser, less uniform/compact dark mass with diffuse or crumbly edges. loose_fecal_pellet is an elongated, breaking-apart fecal cast, while aggregate_light is amorphous marine-snow detritus with no pellet shape." },
  },

  // ---- Out-of-focus / blurred particles ----
  {
    id: "particle_blur", label: "Particle (blur)", category: "Out-of-focus / blurred particles", count: 2107, hasProfile: true, isBiological: false,
    note: "Rounded, soft-edged, out-of-focus dark blob — no sharp edges anywhere in the crop.",
    description: "A catch-all for any discrete dark particle that is far enough out of the ISIIS camera's focal plane to lose all sharp edges. The object itself could be an aggregate, a pellet, or something else entirely — what defines this class is the blur, not the particle's identity.",
    images: { good: "assets/species/particle_blur/good.jpg", features: "assets/species/particle_blur/features.jpg", similar: "assets/species/particle_blur/similar.jpg" },
    similar: { id: "aggregate_dense", reason: "Both are solid, roughly round dark masses. particle_blur is out of the focal plane with soft, blurred edges, while aggregate_dense is the same kind of object in focus with a sharp, defined edge." },
  },

  // ---- Non-biological / imaging artifacts ----
  {
    id: "artifact", label: "Artifact", category: "Non-biological / imaging artifacts", count: 9526, hasProfile: true, isBiological: false,
    note: "Faint, indistinct, low-contrast smudge with no clear boundary — not a real particle.",
    description: "Optical or processing artifacts: faint, indistinct, low-contrast smudges with no clear boundary that don't correspond to any real particle in the water. Labeled so they can be filtered out of ecological counts downstream.",
    images: { good: "assets/species/artifact/good.jpg", features: "assets/species/artifact/features.jpg", similar: "assets/species/artifact/similar.jpg" },
    similar: { id: "noise", reason: "Both are faint, low-contrast marks that don't correspond to a real particle. artifact is a larger, more indistinct smudge with no clear boundary, while noise is smaller, sharper, soft circular specks." },
  },
  {
    id: "noise", label: "Noise", category: "Non-biological / imaging artifacts", count: 2383, hasProfile: true, isBiological: false,
    note: "Small faint speck(s)/soft circular blobs, sensor or optical noise rather than a discrete object.",
    description: "Sensor or optical noise: small, faint specks or soft circular blobs produced by the imaging system itself rather than by anything in the water.",
    images: { good: "assets/species/noise/good.jpg", features: "assets/species/noise/features.jpg", similar: "assets/species/noise/similar.jpg" },
    similar: { id: "artifact", reason: "Both are faint marks that don't correspond to a real particle. noise is small, sharp, soft circular specks, while artifact is a larger, more indistinct, low-contrast smudge with no clear boundary." },
  },
  {
    id: "bubble", label: "Bubble", category: "Non-biological / imaging artifacts", count: 1501, hasProfile: true, isBiological: false,
    note: "Perfectly circular, uniformly solid black disc with a sharp, clean edge.",
    description: "Air bubbles introduced by the instrument or its housing as it moves through the water. They're one of the easier artifacts to identify reliably: a perfectly circular, uniformly solid black disc with a sharp, clean edge — real organisms almost never produce that exact combination.",
    images: { good: "assets/species/bubble/good.jpg", features: "assets/species/bubble/features.jpg", similar: "assets/species/bubble/similar.jpg" },
    similar: { id: "centric_diatom", reason: "Both are small, solid, dark, roughly circular shapes. The bubble is a perfectly circular, featureless, uniformly black disc, while the diatom shows a visible cell-wall rim and slightly irregular internal texture." },
  },
  {
    id: "football", label: "Football", category: "Non-biological / imaging artifacts", count: 120, hasProfile: true, isBiological: false,
    note: "Oval, out-of-focus artifact resembling an American football, with bright/dark banding.",
    description: "An out-of-focus optical artifact with a characteristic oval shape and bright/dark banding, resembling an American football — most likely a bubble or piece of debris on the optics passing through an intermediate focal distance, rather than anything biological.",
    images: { good: "assets/species/football/good.jpg", features: "assets/species/football/features.jpg", similar: "assets/species/football/similar.jpg" },
    similar: { id: "bubble", reason: "Both are thought to originate from air bubbles or debris on the optics. The bubble is sharply in focus, perfectly circular, and uniformly black, while the football is out of focus, oval, and shows bright/dark banding rather than a solid fill." },
  },
  {
    id: "density", label: "Density", category: "Non-biological / imaging artifacts", count: 1052, hasProfile: true, isBiological: false,
    note: "Diffuse, large-scale mottled shading across the whole frame (a water density/optical gradient), no discrete object.",
    description: "A water density or refractive-index gradient (e.g. a thermocline or salinity interface) crossing the imaging volume, seen as diffuse, large-scale mottled shading across the whole frame rather than any discrete object.",
    images: { good: "assets/species/density/good.jpg", features: "assets/species/density/features.jpg", similar: "assets/species/density/similar.jpg" },
    similar: { id: "bloom", reason: "Both are frame-wide labels rather than single discrete objects. density is a smooth, mottled shading gradient with no distinguishable particles, while bloom shows many small distinguishable round particles/cells scattered throughout." },
  },
  {
    id: "string", label: "String", category: "Non-biological / imaging artifacts", count: 330, hasProfile: true, isBiological: false,
    note: "Thin straight or gently curved line(s) crossing the frame — a fiber or optical streak, not attached to any organism/aggregate.",
    description: "A thin fiber or optical streak crossing the frame — debris, a stray strand, or a scratch/artifact on the optics — straight or gently curved, and not attached to any organism or aggregate (contrast with `aggregate_string`, where a similar strand has marine snow collected on it).",
    images: { good: "assets/species/string/good.jpg", features: "assets/species/string/features.jpg", similar: "assets/species/string/similar.jpg" },
    similar: { id: "aggregate_string", reason: "Both feature a thin string-like strand. The `string` class is a bare fiber/streak with nothing attached, while `aggregate_string` is the same kind of strand with a marine-snow mass collected on it." },
  },
];
