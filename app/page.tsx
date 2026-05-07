export default async function Home() {
  const res = await fetch("http://localhost:3000/api/permits", {
    cache: "no-store",
  });

  const permits = await res.json();

  return (
    <main style={{ padding: "2rem" }}>
      <h1>GroundBreak Leads</h1>

      {permits.map((permit: any) => (
        <div
          key={permit.id}
          style={{
            border: "1px solid #ddd",
            padding: "1rem",
            marginBottom: "1rem",
            borderRadius: "8px",
          }}
        >
          <h2>
            {permit.city} — ${Number(permit.estimated_cost).toLocaleString()}
          </h2>

          <p><strong>Permit:</strong> {permit.permit_number}</p>
          <p><strong>Work:</strong> {permit.work_class}</p>
          <p><strong>Address:</strong> {permit.address || "No address listed"}</p>
          <p><strong>Contractor:</strong> {permit.contractor || "Check comments"}</p>
          <p><strong>Email:</strong> {permit.contractor_email || "Check comments"}</p>
          <p><strong>Description:</strong> {permit.description}</p>
        </div>
      ))}
    </main>
  );
}