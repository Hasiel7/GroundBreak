/**
 * GroundBreak — Raleigh Permit Fetcher
 *
 * Pulls commercial building permits from Raleigh Open Data (last 60 days)
 * and saves HVAC-relevant leads to a CSV.
 *
 * Usage:
 *   npx tsx scripts/fetch-permits.ts
 */

import fs from "fs";
import path from "path";

const API_URL =
  "https://services.arcgis.com/v400IkDOw1ad7Yad/arcgis/rest/services/Building_Permits_Issued_Past_180_Days/FeatureServer/0/query";

const DAYS_BACK = 60;
const MIN_PROJECT_COST = 250_000;

// ─── FILTERS ──────────────────────────────────────────────────────────────────
// Exact censuslanduse values from the Raleigh API that are relevant

const COMMERCIAL_USES = [
  "ADDITION/ALTERATION NONRESIDENTIAL BLDG",
  "INDUSTRIAL BUILDING",
  "OFFICE, BANK, AND PROFESSIONAL BUILDING",
  "SCHOOL AND OTHER EDUCATIONAL BUILDING",
  "STORE AND MERCANTILE BUILDING",
  "AMUSEMENT & RECREATIONAL BUILDING",
  "CHURCH OR OTHER RELIGIOUS BUILDING",
  "HOTEL, MOTEL AND TOURIST CABIN",
  "FIVE OR MORE FAMILY BUILDING",
];

function isLead(a: Record<string, any>): boolean {
  const use = (a.censuslanduse || "").trim();
  if (!COMMERCIAL_USES.some((c) => use.includes(c))) return false;
  if ((Number(a.estprojectcost) || 0) < MIN_PROJECT_COST) return false;
  return true;
}

// ─── FETCH PERMITS ────────────────────────────────────────────────────────────

async function fetchPermits() {
  const since = new Date();
  since.setDate(since.getDate() - DAYS_BACK);

  const params = new URLSearchParams({
    where: `issueddate >= '${since.toISOString()}'`,
    outFields: "*",
    resultRecordCount: "2000",
    orderByFields: "issueddate DESC",
    f: "json",
    returnGeometry: "true",
    outSR: "4326",
  });

  console.log(`Fetching permits from the last ${DAYS_BACK} days...`);
  const res = await fetch(`${API_URL}?${params}`);
  const data = await res.json();

  if (data.error) {
    console.error("API error:", data.error);
    return;
  }

  const leads = (data.features || []).filter((f: any) => isLead(f.attributes));
  console.log(`Total returned: ${data.features?.length || 0} | Leads: ${leads.length}\n`);

  if (leads.length === 0) {
    console.log("No leads found. Try adjusting filters or increasing DAYS_BACK.");
    return;
  }

  // Build CSV
  const clean = (s: string) => `"${(s || "").replace(/"/g, '""').replace(/[\r\n]+/g, " ")}"`;

  const headers = [
    "Permit Number", "Work Class", "Address", "City",
    "Contractor", "Contractor Phone", "Contractor Email", "Contractor License",
    "Estimated Cost", "Total SqFt", "Stories", "Issue Date",
    "Description", "Building Type", "Latitude", "Longitude",
  ];

  const rows = leads.map((f: any) => {
    const a = f.attributes;
    return [
      a.permitnum || "",
      a.workclass || "",
      clean(a.originaladdress1),
      a.originalcity || "",
      clean(a.contractorcompanyname),
      a.contractorphone || "",
      a.contractoremail || "",
      a.contractorlicnum || "",
      a.estprojectcost || "",
      a.totalsqft || "",
      a.numberstories || "",
      a.issueddate ? new Date(a.issueddate).toLocaleDateString() : "",
      clean(a.proposedworkdescription),
      clean(a.censuslanduse),
      a.latitude_perm || "",
      a.longitude_perm || "",
    ].join(",");
  });

  // Write CSV
  const outDir = path.join(process.cwd(), "data");
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  const filename = `raleigh-permits-${new Date().toISOString().split("T")[0]}.csv`;
  const outPath = path.join(outDir, filename);
  fs.writeFileSync(outPath, [headers.join(","), ...rows].join("\n"));
  console.log(`Saved ${leads.length} leads to: ${outPath}\n`);

  // Preview top 5
  console.log("Top 5 by estimated value:\n");
  leads
    .map((f: any) => f.attributes)
    .sort((a: any, b: any) => (Number(b.estprojectcost) || 0) - (Number(a.estprojectcost) || 0))
    .slice(0, 5)
    .forEach((a: any) => {
      const cost = Number(a.estprojectcost) || 0;
      console.log(`  $${cost.toLocaleString().padEnd(15)} ${(a.originaladdress1 || "?").padEnd(40)} ${a.contractorcompanyname || "no contractor"}`);
    });
}

fetchPermits();