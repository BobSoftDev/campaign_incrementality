<h1>CRM Campaign Effectiveness &amp; Incrementality</h1>

<p>
  <strong>Role lens:</strong> Senior CRM Data Analyst<br/>
  <em>(analytics-only; no ML, no attribution)</em>
</p>

<hr/>

<h2>1. Purpose (what this is)</h2>
<p>
  This project measures the <strong>true incremental impact</strong> of CRM campaigns using a transparent
  <strong>exposed vs. eligible holdout</strong> design.
  It produces <strong>executive-safe KPIs</strong> and a <strong>multi-page Streamlit dashboard</strong>
  that supports <em>scale / optimize / stop</em> decisions.
</p>

<h2>2. Why it exists (business problem)</h2>
<p>Observed campaign performance is misleading because:</p>
<ul>
  <li>Many customers would purchase anyway (<em>natural demand</em>)</li>
  <li>Targeting selects high-intent customers (<em>selection bias</em>)</li>
  <li>Exposed-only reporting confuses correlation with causation</li>
</ul>
<p>
  This project solves that by explicitly estimating the <strong>counterfactual</strong> using a holdout group.
</p>

<h2>3. What the app delivers (decision-first outputs)</h2>
<ul>
  <li>Campaign ranking by <strong>Incremental Revenue</strong> (primary decision metric)</li>
  <li>Deep dive into exposed vs. holdout KPI drivers</li>
  <li>Segment-level incrementality to guide suppression and focus</li>
  <li>Customer-level distribution checks to validate outlier risk</li>
  <li>Definitions &amp; methodology page for auditability</li>
</ul>

<h2>4. Data flow (raw → processed → marts → dashboard)</h2>

<h3>data/raw/</h3>
<ul>
  <li>dim_customers.csv</li>
  <li>dim_campaigns.csv</li>
  <li>fact_eligibility.csv</li>
  <li>fact_exposure.csv</li>
  <li>fact_transactions.csv</li>
</ul>

<h3>data/processed/</h3>
<ul>
  <li>mart_campaign_outcomes.csv<br/>
      <em>(customer × campaign outcomes aligned to attribution window)</em></li>
</ul>

<h3>data/marts/</h3>
<ul>
  <li>mart_kpis_campaign.csv</li>
  <li>mart_kpis_segment.csv</li>
  <li>mart_campaign_outcomes_light.csv<br/>
      <em>(dashboard reads these only)</em></li>
</ul>

<h2>5. How to run (Windows-safe)</h2>

<pre>
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python scripts/run_all.py
streamlit run app/app.py
</pre>

<h2>6. Assumptions and uncertainty (explicit)</h2>

<ul>
  <li>
    <strong>Synthetic-data mode (portfolio):</strong>
    Data is simulated to demonstrate methodology and product structure.
    Any numerical uplift values are illustrative, not factual.
  </li>
  <li>
    <strong>Causal validity depends on holdout integrity:</strong>
    Results are causal only if holdout customers are truly eligible and not exposed
    (no leakage) and if overlap/interference is controlled.
  </li>
  <li>
    <strong>Attribution window sensitivity:</strong>
    Incrementality depends on the chosen window (default 14 days).
    Changing the window can change uplift magnitude and even sign.
  </li>
</ul>

<h2>7. Reference links</h2>
<ul>
  <li><a href="https://docs.streamlit.io/">Streamlit documentation</a></li>
  <li><a href="https://docs.streamlit.io/develop/concepts/multipage-apps">Streamlit multipage apps</a></li>
  <li><a href="https://pandas.pydata.org/docs/">Pandas documentation</a></li>
  <li><a href="https://numpy.org/doc/">NumPy documentation</a></li>
  <li><a href="https://pyyaml.org/wiki/PyYAMLDocumentation">PyYAML documentation</a></li>
</ul>

<hr/>

<h2>KPI Definitions &amp; Methodology (audit-ready)</h2>

<h3>Core definitions</h3>
<ul>
  <li><strong>Eligible customer:</strong> Satisfies campaign rules before send.</li>
  <li><strong>Exposed customer:</strong> Eligible customer with delivered message (<code>delivered_flag = 1</code>).</li>
  <li><strong>Holdout customer:</strong> Eligible customer intentionally not delivered (<code>control_flag = 1</code>).</li>
</ul>

<p><strong>Important:</strong> Selection ≠ exposure.</p>

<h3>Outcome definitions</h3>
<ul>
  <li><strong>Attribution window:</strong> Fixed window (default 14 days)</li>
  <li><strong>Exposed anchor:</strong> Delivered timestamp</li>
  <li><strong>Holdout anchor:</strong> Campaign start timestamp</li>
  <li><strong>Conversion:</strong> ≥1 transaction in the window</li>
  <li><strong>Revenue in window:</strong> Sum of transaction revenue in window</li>
</ul>

<h3>Mandatory formulas (as implemented)</h3>

<pre>
CR_exposed   = C_E / N_E
CR_holdout  = C_H / N_H

RPC_exposed = R_E / N_E
RPC_holdout = R_H / N_H

CR uplift   = CR_exposed - CR_holdout
RPC uplift  = RPC_exposed - RPC_holdout

Incremental Revenue = (RPC_exposed - RPC_holdout) × N_E
</pre>

<h3>Governance rules (when to trust the result)</h3>
<ul>
  <li>Holdout is eligible and not exposed (no leakage)</li>
  <li>Sample sizes exceed minimum threshold</li>
  <li>Overlap within attribution window is below governance threshold</li>
</ul>

<p>
  Treat results as <strong>directional</strong> if any governance condition fails.
</p>

<hr/>

<h2>Executive Narrative</h2>

<h3>What worked</h3>
<ul>
  <li>Single source of truth for incrementality</li>
  <li>Decision-first reporting (scale / optimize / stop)</li>
  <li>Auditable, repeatable pipeline</li>
</ul>

<h3>What didn’t work (by design)</h3>
<ul>
  <li>No cross-channel attribution models</li>
  <li>No ML-based targeting optimization</li>
  <li>Results degrade if real-world leakage exists</li>
</ul>

<h3>Scale / Optimize / Stop logic</h3>

<p><strong>Scale when:</strong></p>
<ul>
  <li>Incremental revenue is materially positive</li>
  <li>Governance flags are clean</li>
  <li>Uplift is not driven by outliers</li>
</ul>

<p><strong>Optimize when:</strong></p>
<ul>
  <li>Overall incrementality ≈ 0 but some segments are positive</li>
  <li>Results are window-sensitive</li>
  <li>Overlap is moderate</li>
</ul>

<p><strong>Stop when:</strong></p>
<ul>
  <li>Incremental revenue is consistently negative</li>
  <li>Holdout performs similarly to exposed</li>
  <li>Governance issues cannot be fixed quickly</li>
</ul>

<p>
  <em>Uncertainty note:</em> In synthetic mode, these conclusions demonstrate the framework,
  not real campaign claims.
</p>
