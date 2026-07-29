// Data for the interactive ISIIS labeling guide.
// Each entry corresponds to one label class used in the `Baseline` dataset.
//
// `hasProfile: true` classes have a full 3-image profile (good / features / similar)
// plus a written description and WoRMS taxonomy. Everything else still shows in the
// nav with its single reference image and short visual note, marked "profile coming soon".
//
// Taxonomy fields (rank/authority/aphiaID/environment/vernacular) are sourced from WoRMS
// (https://www.marinespecies.org/aphia.php?p=taxlist) via the AphiaID linked in `wormsUrl`.
// WoRMS records are taxonomic (name, rank, classification, environment) rather than
// prose descriptions, so the `description` text for each class is written by hand from
// general planktonic biology, not copied from WoRMS.

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
  { id: "acantharia", label: "Acantharia", category: "Radiolarians & rhizarians", count: 9,
    note: "Small dark central capsule with a few long, straight, radiating spicules.", hasProfile: false },
  { id: "rhizaria", label: "Rhizaria", category: "Radiolarians & rhizarians", count: 4,
    note: "Dense dark irregular clump with short spines radiating from the edge; more compact/less symmetric than acantharia.", hasProfile: false },
  { id: "phaeodarian_A", label: "Phaeodarian A", category: "Radiolarians & rhizarians", count: 36,
    note: "Solid dark oval body with a fine textured/granular interior, no long spines.", hasProfile: false },
  { id: "phaeodarian_B", label: "Phaeodarian B", category: "Radiolarians & rhizarians", count: 20,
    note: "Dark irregular star-shaped mass with short jagged spines all around.", hasProfile: false },
  { id: "phaeodarian_C", label: "Phaeodarian C", category: "Radiolarians & rhizarians", count: 8,
    note: "Round lattice/honeycomb \"shell\" pattern surrounding a small dark central body.", hasProfile: false },
  { id: "phaeodarian_D", label: "Phaeodarian D", category: "Radiolarians & rhizarians", count: 4,
    note: "Round body with fine radial striations (sunburst pattern) around a solid dark center, no protruding spines.", hasProfile: false },
  { id: "phaeodarian_E", label: "Phaeodarian E", category: "Radiolarians & rhizarians", count: 4,
    note: "Dark center with a moderate number of long, thin, evenly spaced spines — sparser than acantharia.", hasProfile: false },
  { id: "phaeodarian_F", label: "Phaeodarian F", category: "Radiolarians & rhizarians", count: 5,
    note: "Small dark center with many short, fine spines giving a fuzzy/starburst edge.", hasProfile: false },

  // ---- Diatoms ----
  { id: "centric_diatom", label: "Centric diatom", category: "Diatoms", count: 27,
    note: "Single small, solid, roundish/barrel-shaped dark cell, no chain.", hasProfile: false },
  { id: "diatom_chain_straight", label: "Diatom chain (straight)", category: "Diatoms", count: 725,
    note: "Row of small dark cells strung together in a straight or gently curved line.", hasProfile: false },
  { id: "diatom_chain_spiral", label: "Diatom chain (spiral)", category: "Diatoms", count: 2262,
    note: "Chain of cells curled into a tight spiral/coil rather than a line.", hasProfile: false },
  { id: "diatom_chain_spines", label: "Diatom chain (spines)", category: "Diatoms", count: 999,
    note: "Chain of cells with fine spines/setae radiating outward, giving a fan-like or bristly appearance.", hasProfile: false },

  // ---- Gelatinous zooplankton ----
  { id: "larvacean", label: "Larvacean", category: "Gelatinous zooplankton", count: 355,
    note: "Dark, S-curved or comma-shaped elongated body, tadpole-like.", hasProfile: false },
  {
    id: "salp", label: "Salp", category: "Gelatinous zooplankton", count: 5, hasProfile: true,
    note: "Translucent barrel/oval body with visible ring-like muscle bands and a small dark internal mass; often a thin trailing thread.",
    description: "Salps are barrel-shaped, gelatinous pelagic tunicates that swim by jet propulsion, pumping water through the body wall with each muscular contraction. They often form long chains by asexual budding, though isolated solitary individuals (as labeled here) are common in ISIIS imagery. In shadowgraph, a salp shows a translucent oval/barrel outline with clear ring-like muscle bands encircling the body and a small dark internal mass (the visceral \"nucleus\"), often with a thin trailing thread of mucus or fecal material.",
    taxonomy: { rank: "Order", name: "Salpida", authority: "Forbes, 1853", aphiaId: 137214, phylum: "Chordata", className: "Thaliacea", environment: "Marine (pelagic tunicates)", vernacular: "salps" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=137214",
    images: { good: "assets/species/salp/good.jpg", features: "assets/species/salp/features.jpg", similar: "assets/species/salp/similar.jpg" },
    similar: { id: "siphonophore", reason: "Both appear as an oval/barrel translucent body with internal banding or canals. Check for the salp's ring-like muscle bands and lack of a trailing tentacle, versus the siphonophore's radiating canal pattern and long trailing strand." },
  },
  {
    id: "siphonophore", label: "Siphonophore", category: "Gelatinous zooplankton", count: 38, hasProfile: true,
    note: "Similar oval/barrel float with striations, usually paired with a long thin trailing tentacle/strand extending well outside the body.",
    description: "Siphonophores are colonial hydrozoans — a chain of specialized, genetically identical zooids (each doing one job: floating, swimming, feeding, or reproducing) that function together as a single organism. The label in this dataset mainly corresponds to physonect siphonophores viewed by their gas-filled pneumatophore or a nectophore (swimming bell): a translucent, fan- or bell-shaped structure with fine radiating canals and scattered dark pigment spots, usually trailing one or more long, thin tentacles extending well outside the body used to capture prey.",
    taxonomy: { rank: "Order", name: "Siphonophorae", authority: "Eschscholtz, 1829", aphiaId: 1371, phylum: "Cnidaria", className: "Hydrozoa", environment: "Primarily marine/pelagic", vernacular: "siphonophores" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=1371",
    images: { good: "assets/species/siphonophore/good.jpg", features: "assets/species/siphonophore/features.jpg", similar: "assets/species/siphonophore/similar.jpg" },
    similar: { id: "salp", reason: "Both appear as an oval/barrel translucent body with internal banding or canals. Check for the siphonophore's radiating canal pattern, pigment spots, and long trailing tentacle, versus the salp's evenly spaced ring-like muscle bands and lack of a tentacle." },
  },
  { id: "jelly", label: "Jelly", category: "Gelatinous zooplankton", count: 15,
    note: "Thin, irregular membranous outline with fine trailing tentacle-like strands; no compact solid body.", hasProfile: false },
  { id: "ctenophore", label: "Ctenophore", category: "Gelatinous zooplankton", count: 7,
    note: "Body showing parallel curved comb-row ridges/banding.", hasProfile: false },

  // ---- Crustaceans & other animals ----
  {
    id: "copepod", label: "Copepod", category: "Crustaceans & other animals", count: 1358, hasProfile: true,
    note: "Small dark comma/teardrop-shaped body with thin trailing antennae/tail setae.",
    description: "Copepods are small crustaceans and among the most numerous multicellular animals on Earth, forming a dominant part of the mesozooplankton in every ocean. Most specimens labeled in this set are calanoid copepods, recognizable in shadowgraph imagery by a dark, comma- or teardrop-shaped cephalothorax (head + thorax), a pair of long antennae projecting forward and outward, and a narrower segmented abdomen trailing into fine caudal setae (\"tail\" bristles) used for sensing and swimming.",
    taxonomy: { rank: "Class", name: "Copepoda", authority: "Milne Edwards, 1840", aphiaId: 1080, phylum: "Arthropoda", className: "Copepoda (subphylum Crustacea)", environment: "Marine, brackish, fresh, terrestrial", vernacular: "copepods" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=1080",
    images: { good: "assets/species/copepod/good.jpg", features: "assets/species/copepod/features.jpg", similar: "assets/species/copepod/similar.jpg" },
    similar: { id: "molt", reason: "A shed copepod exoskeleton keeps the exact same silhouette — antennae, body outline, tail setae — but reads as translucent/empty in shadowgraph, lacking the solid dark body mass of a living animal." },
  },
  {
    id: "chaetognath", label: "Chaetognath", category: "Crustaceans & other animals", count: 42, hasProfile: true,
    note: "Straight, elongated, torpedo-shaped translucent body tapering to a point (arrow worm).",
    description: "Chaetognaths, or \"arrow worms\", are voracious planktonic predators found throughout the world's oceans. They have a straight, transparent, torpedo-shaped body: a distinct head at one end, a long trunk showing longitudinal muscle bands (visible in shadowgraph as fine striations running the length of the body, with the gut often visible as a darker central line), and a paddle-like tail fin with small hooked grasping spines near the head used to seize prey.",
    taxonomy: { rank: "Phylum", name: "Chaetognatha", authority: null, aphiaId: 2081, phylum: "Chaetognatha", className: "Sagittoidea", environment: "Marine only", vernacular: "arrow worms" },
    wormsUrl: "https://www.marinespecies.org/aphia.php?p=taxdetails&id=2081",
    images: { good: "assets/species/chaetognath/good.jpg", features: "assets/species/chaetognath/features.jpg", similar: "assets/species/chaetognath/similar.jpg" },
    similar: { id: "long_fecal_pellet", reason: "Both are straight, elongated, dark shapes of similar size. Look for the chaetognath's tapered head, tail fin, and internal muscle striations, versus the fecal pellet's uniform width and otherwise featureless interior." },
  },
  { id: "worm", label: "Worm", category: "Crustaceans & other animals", count: 12,
    note: "Elongated segmented body with paired bristle-like parapodia along both sides.", hasProfile: false },
  { id: "molt", label: "Molt", category: "Crustaceans & other animals", count: 5,
    note: "Empty, translucent, articulated exoskeleton outline (legs/antennae visible) with no solid dark body mass — a shed carapace, not a living animal.", hasProfile: false },
  { id: "shrimp", label: "Shrimp", category: "Crustaceans & other animals", count: 7,
    note: "Visible jointed legs/antennae and a segmented body/tail fan.", hasProfile: false },
  { id: "squid", label: "Squid", category: "Crustaceans & other animals", count: 1,
    note: "Large solid dark silhouette with a distinct mantle and fin shape.", hasProfile: false },

  // ---- Colonial algae ----
  { id: "phaeocystis", label: "Phaeocystis", category: "Colonial algae", count: 2328,
    note: "Fuzzy, irregular dark colonial clump, usually trailing one or two long parallel straight strands.", hasProfile: false },

  // ---- Marine snow, aggregates & fecal material ----
  { id: "aggregate", label: "Aggregate", category: "Marine snow, aggregates & fecal material", count: 452,
    note: "Fluffy, irregular dark clump with visible fine trailing strands/fibers.", hasProfile: false },
  { id: "aggregate_dense", label: "Aggregate (dense)", category: "Marine snow, aggregates & fecal material", count: 5815,
    note: "Solid, compact, uniformly dark blob with little internal structure — most common class in the set.", hasProfile: false },
  { id: "aggregate_light", label: "Aggregate (light)", category: "Marine snow, aggregates & fecal material", count: 287,
    note: "Looser, lower-contrast/more translucent version of `aggregate` — less dense, more diffuse edges.", hasProfile: false },
  { id: "aggregate_string", label: "Aggregate (string)", category: "Marine snow, aggregates & fecal material", count: 740,
    note: "Aggregate mass hanging off a single long, thin string/strand.", hasProfile: false },
  { id: "bloom", label: "Bloom", category: "Marine snow, aggregates & fecal material", count: 21311,
    note: "Frame scattered with many small round particles/cells rather than one discrete object — most common class overall.", hasProfile: false },
  { id: "long_fecal_pellet", label: "Fecal pellet (long)", category: "Marine snow, aggregates & fecal material", count: 414,
    note: "Long, slender, solid dark cylindrical pellet, uniform width.", hasProfile: false },
  { id: "short_fecal_pellet", label: "Fecal pellet (short)", category: "Marine snow, aggregates & fecal material", count: 11,
    note: "Same as long_fecal_pellet but short/oval rather than elongated.", hasProfile: false },
  { id: "loose_fecal_pellet", label: "Fecal pellet (loose)", category: "Marine snow, aggregates & fecal material", count: 221,
    note: "Irregular, crumbly-edged dark mass — less uniform/compact than a solid pellet.", hasProfile: false },

  // ---- Out-of-focus / blurred particles ----
  { id: "particle_blur", label: "Particle (blur)", category: "Out-of-focus / blurred particles", count: 2107,
    note: "Rounded, soft-edged, out-of-focus dark blob — no sharp edges anywhere in the crop. Compare against aggregate_dense, which is similar but in-focus with a defined edge.", hasProfile: false },
  { id: "long_particle_blur", label: "Particle (long blur)", category: "Out-of-focus / blurred particles", count: 313,
    note: "Same as particle_blur but elongated/curved rather than round.", hasProfile: false },

  // ---- Non-biological / imaging artifacts ----
  { id: "artifact", label: "Artifact", category: "Non-biological / imaging artifacts", count: 9526,
    note: "Faint, indistinct, low-contrast smudge with no clear boundary — not a real particle.", hasProfile: false },
  { id: "noise", label: "Noise", category: "Non-biological / imaging artifacts", count: 2383,
    note: "Small faint speck(s)/soft circular blobs, sensor or optical noise rather than a discrete object.", hasProfile: false },
  { id: "bubble", label: "Bubble", category: "Non-biological / imaging artifacts", count: 1501,
    note: "Perfectly circular, uniformly solid black disc with a sharp, clean edge.", hasProfile: false },
  { id: "football", label: "Football", category: "Non-biological / imaging artifacts", count: 120,
    note: "Oval, out-of-focus artifact resembling an American football, with bright/dark banding.", hasProfile: false },
  { id: "density", label: "Density", category: "Non-biological / imaging artifacts", count: 1052,
    note: "Diffuse, large-scale mottled shading across the whole frame (a water density/optical gradient), no discrete object.", hasProfile: false },
  { id: "string", label: "String", category: "Non-biological / imaging artifacts", count: 330,
    note: "Thin straight or gently curved line(s) crossing the frame — a fiber or optical streak, not attached to any organism/aggregate.", hasProfile: false },
];
