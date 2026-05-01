import { useEffect, useMemo, useState } from "react";
import { ComposableMap, Geographies, Geography } from "react-simple-maps";

const geographyUrl = "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json";
const defaultPanel = {
  iso3: "",
  country: "Select a country",
  currentGovernor: "",
  records: [],
  totalGovernors: 0,
  sourceUrl: "",
};

function App() {
  const [mapData, setMapData] = useState({});
  const [mapNameIndex, setMapNameIndex] = useState({});
  const [summary, setSummary] = useState(null);
  const [selected, setSelected] = useState(defaultPanel);
  const [hovered, setHovered] = useState(null);

  useEffect(() => {
    fetch("/governors_by_country.json")
      .then((res) => res.json())
      .then((data) => {
        const countries = data.countries || {};
        setMapData(countries);
        setSummary(data.summary || null);
        const byName = {};
        Object.values(countries).forEach((entry) => {
          if (entry.mapCountryName) {
            byName[entry.mapCountryName] = entry;
          }
        });
        setMapNameIndex(byName);
      })
      .catch((err) => {
        console.error("Failed to load map data", err);
      });
  }, []);

  function selectCountry(geo) {
    const countryName = geo.properties.name;
    const data = mapNameIndex[countryName] || null;
    if (!data) {
      setSelected({
        ...defaultPanel,
        iso3: "",
        country: countryName || "Unknown country",
      });
      return;
    }
    setSelected(data);
  }

  return (
    <div className="app-shell">
      <header className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Central Bank Governors</p>
          <h1>World map of historical leadership</h1>
          <p className="lede">
            Hover to preview a country. Click to open the historical list of governors.
          </p>
        </div>
        <div className="hero-actions">
          <a className="action-button primary" href="/kof_governors_with_sources.csv" download>
            Download CSV
          </a>
          <a
            className="action-button secondary"
            href="https://kof.ethz.ch/en/data/data-on-central-bank-governors.html"
            target="_blank"
            rel="noreferrer"
          >
            KOF Dataset
          </a>
        </div>
      </header>

      {summary ? (
        <section className="summary-strip">
          <div className="summary-card">
            <span>Countries</span>
            <strong>{summary.totalCountries}</strong>
          </div>
          <div className="summary-card">
            <span>Total records</span>
            <strong>{summary.totalRecords}</strong>
          </div>
          <div className="summary-card">
            <span>Current governors</span>
            <strong>{summary.currentGovernors}</strong>
          </div>
          <div className="summary-card">
            <span>Historical governors</span>
            <strong>{summary.historicalGovernors}</strong>
          </div>
          <div className="summary-card">
            <span>Rows with source URL</span>
            <strong>{summary.withSourceUrl}</strong>
          </div>
        </section>
      ) : null}

      <main className="layout">
        <section className="map-card">
          <div className="map-toolbar">
            <div>
              <strong>Map legend</strong>
              <p>Dark countries have at least one matched governor record.</p>
            </div>
            <div className="hover-box">
              {hovered ? (
                <>
                  <strong>{hovered.country}</strong>
                  <span>{hovered.totalGovernors} governors</span>
                  <span>{hovered.currentGovernor || "No current governor flagged"}</span>
                </>
              ) : (
                <span>Hover a country</span>
              )}
            </div>
          </div>

          <ComposableMap projection="geoEqualEarth" className="world-map">
            <Geographies geography={geographyUrl}>
              {({ geographies }) =>
                geographies.map((geo) => {
                  const countryName = geo.properties.name;
                  const data = mapNameIndex[countryName] || null;
                  const fill = data ? "#103f3f" : "#d7d1c4";
                  return (
                    <Geography
                      key={geo.rsmKey}
                      geography={geo}
                      fill={fill}
                      stroke="#f5f0e5"
                      strokeWidth={0.4}
                      style={{
                        default: { outline: "none" },
                        hover: { fill: "#d06d3e", outline: "none", cursor: "pointer" },
                        pressed: { fill: "#d06d3e", outline: "none" },
                      }}
                      onMouseEnter={() => {
                        if (data) {
                          setHovered(data);
                        } else {
                          setHovered({
                            country: countryName || "Unknown country",
                            totalGovernors: 0,
                            currentGovernor: "",
                          });
                        }
                      }}
                      onMouseLeave={() => setHovered(null)}
                      onClick={() => selectCountry(geo)}
                    />
                  );
                })
              }
            </Geographies>
          </ComposableMap>
        </section>

        <aside className="panel">
          <div className="panel-header">
            <p className="eyebrow">Country detail</p>
            <h2>{selected.country}</h2>
            {selected.iso3 ? <span className="pill">{selected.iso3}</span> : null}
          </div>

          {selected.totalGovernors ? (
            <>
              <div className="stats-grid">
                <div>
                  <span>Total governors</span>
                  <strong>{selected.totalGovernors}</strong>
                </div>
                <div>
                  <span>Current governor</span>
                  <strong>{selected.currentGovernor || "N/A"}</strong>
                </div>
              </div>

              {selected.sourceUrl ? (
                <p className="source-line">
                  <a href={selected.sourceUrl} target="_blank" rel="noreferrer">
                    Official source
                  </a>
                </p>
              ) : null}

              <div className="records-list">
                {selected.records.map((record) => (
                  <article key={`${record.name}-${record.start_year}-${record.end_year}`} className="record-card">
                    <strong>{record.name}</strong>
                    <span>{record.position}</span>
                    <span>
                      {record.start_year} - {record.end_year}
                    </span>
                    <span className={`status ${record.status === "Actual" ? "current" : "historical"}`}>
                      {record.status}
                    </span>
                  </article>
                ))}
              </div>
            </>
          ) : (
            <p className="empty-panel">
              No governor records are available for this country in the processed CSV.
            </p>
          )}
        </aside>
      </main>
    </div>
  );
}

export default App;
