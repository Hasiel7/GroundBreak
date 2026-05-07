/**
 * GroundBreak — Permit Fetcher (Raleigh + Durham → Supabase)
 *
 * Pulls commercial building permits from both cities
 * and upserts them into the Supabase permits table.
 *
 * Usage:
 *   npx tsx scripts/fetch-permits.ts
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const DAYS_BACK = 30;
const MIN_COST = 250_000;

// ─── SUPABASE ─────────────────────────────────────────────────────────────────

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env");
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// ─── SHARED TYPES ─────────────────────────────────────────────────────────────

interface Lead {
  city: string;
  permit_number: string;
  work_class: string;
  address: string;
  contractor: string;
  contractor_phone: string;
  contractor_email: string;
  estimated_cost: number;
  sqft: number;
  stories: number;
  issue_date: string | null;
  description: string;
  building_type: string;
  comments: string;
  latitude: number | null;
  longitude: number | null;
}

// ─── RALEIGH ──────────────────────────────────────────────────────────────────

const RALEIGH_URL =
  "https://services.arcgis.com/v400IkDOw1ad7Yad/arcgis/rest/services/Building_Permits_Issued_Past_180_Days/FeatureServer/0/query";

const RALEIGH_COMMERCIAL_USES = [
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

async function fetchRaleigh(): Promise<Lead[]> {
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

  console.log("  Fetching Raleigh...");
  const res = await fetch(`${RALEIGH_URL}?${params}`);
  const data = await res.json();

  if (data.error) {
    console.error("  Raleigh API error:", data.error);
    return [];
  }

  const leads: Lead[] = [];

  for (const f of data.features || []) {
    const a = f.attributes;
    const use = (a.censuslanduse || "").trim();
    if (!RALEIGH_COMMERCIAL_USES.some((c) => use.includes(c))) continue;
    if ((Number(a.estprojectcost) || 0) < MIN_COST) continue;

    leads.push({
      city: "Raleigh",
      permit_number: a.permitnum || "",
      work_class: a.workclass || "",
      address: a.originaladdress1 || "",
      contractor: a.contractorcompanyname || "",
      contractor_phone: a.contractorphone || "",
      contractor_email: a.contractoremail || "",
      estimated_cost: Number(a.estprojectcost) || 0,
      sqft: Number(a.totalsqft) || 0,
      stories: Number(a.numberstories) || 0,
      issue_date: a.issueddate ? new Date(a.issueddate).toISOString().split("T")[0] : null,
      description: a.proposedworkdescription || "",
      building_type: (a.censuslanduse || "").trim(),
      comments: "",
      latitude: a.latitude_perm || null,
      longitude: a.longitude_perm || null,
    });
  }

  console.log(`  Raleigh: ${data.features?.length || 0} total → ${leads.length} leads`);
  return leads;
}

// ─── DURHAM ───────────────────────────────────────────────────────────────────

const DURHAM_URL =
  "https://webgis.durhamnc.gov/server/rest/services/PublicServices/Inspections/MapServer/12/query";

const DURHAM_ACTIVITIES = [
  "New", "Addition", "Shell Only", "Re-Hab",
  "Interior Alterations", "Change of Occupancy",
];

async function fetchDurham(): Promise<Lead[]> {
  const since = new Date();
  since.setDate(since.getDate() - DAYS_BACK);
  const dateStr = since.toISOString().split("T")[0];

  const where = `BLD_Type='Non-Residential' AND ISSUE_DATE > timestamp '${dateStr} 00:00:00'`;

  const params = new URLSearchParams({
    where,
    outFields: "*",
    resultRecordCount: "2000",
    orderByFields: "ISSUE_DATE DESC",
    f: "json",
    returnGeometry: "true",
    outSR: "4326",
  });

  console.log("  Fetching Durham...");
  const res = await fetch(`${DURHAM_URL}?${params}`);
  const data = await res.json();

  if (data.error) {
    console.error("  Durham API error:", data.error);
    return [];
  }

  const leads: Lead[] = [];

  for (const f of data.features || []) {
    const a = f.attributes;
    const activity = (a.BLDB_ACTIVITY_1 || "").trim();
    if (!DURHAM_ACTIVITIES.some((d) => activity === d)) continue;
    if ((Number(a.BLD_Cost) || 0) < MIN_COST) continue;

    const geo = f.geometry || {};

    leads.push({
      city: "Durham",
      permit_number: a.PermitNum || "",
      work_class: activity,
      address: a.PROJECT_NAME || "",
      contractor: "",
      contractor_phone: "",
      contractor_email: "",
      estimated_cost: Number(a.BLD_Cost) || 0,
      sqft: Number(a.SQFT_FLOOR) || 0,
      stories: 0,
      issue_date: a.ISSUE_DATE ? new Date(a.ISSUE_DATE).toISOString().split("T")[0] : null,
      description: a.DESCRIPTION || "",
      building_type: a.Occupancy || "",
      comments: a.COMMENTS || "",
      latitude: geo.y || null,
      longitude: geo.x || null,
    });
  }

  console.log(`  Durham: ${data.features?.length || 0} total → ${leads.length} leads`);
  return leads;
}

// ─── WRITE TO SUPABASE ────────────────────────────────────────────────────────

async function upsertLeads(leads: Lead[]) {
  console.log(`\n  Writing ${leads.length} leads to Supabase...`);

  // Upsert in batches of 50
  const batchSize = 50;
  let inserted = 0;
  let updated = 0;

  for (let i = 0; i < leads.length; i += batchSize) {
    const batch = leads.slice(i, i + batchSize);

    const { data, error } = await supabase
      .from("permits")
      .upsert(batch, { onConflict: "permit_number" })
      .select();

    if (error) {
      console.error(`  Batch error:`, error.message);
    } else {
      inserted += data?.length || 0;
    }
  }

  console.log(`  Done. ${inserted} permits upserted.`);
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`\nGroundBreak — Fetching permits from the last ${DAYS_BACK} days\n`);

  const [raleigh, durham] = await Promise.all([fetchRaleigh(), fetchDurham()]);
  const allLeads = [...raleigh, ...durham].sort((a, b) => b.estimated_cost - a.estimated_cost);

  console.log(`\n  Combined: ${allLeads.length} leads`);

  if (allLeads.length === 0) {
    console.log("  No leads found. Try increasing DAYS_BACK.");
    return;
  }

  await upsertLeads(allLeads);

  // Preview top 5
  console.log("\n  Top 5 by estimated cost:\n");
  allLeads.slice(0, 5).forEach((l) => {
    console.log(
      `  [${l.city.padEnd(7)}] $${l.estimated_cost.toLocaleString().padEnd(15)} ${(l.address || l.description.slice(0, 40)).padEnd(45)} ${l.contractor || "(check comments)"}`
    );
  });

  console.log("");
}

main();