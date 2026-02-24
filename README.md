<div align="center">
<h1 style="font-size:4em;">CarbonLens AI</h1>
<p><b style="font-size:1.6em;">The Intelligent Scope 3 Governance & Compliance Platform</b></p>
<p style="font-size:1.3em;">
Automating supplier data extraction, verifying carbon evidence, and ensuring SEBI BRSR compliance with AI.
</p>



<p style="font-size:1.4em;">
<a href="#overview">Overview</a> • <a href="#features">Features</a> • <a href="#workflow">Technical Workflow</a> • <a href="#installation">Installation</a>
</p>
</div>

<div style="background:#e6f3ff; padding:15px; border-radius:10px; text-align:center; font-size:1.2em; margin-bottom:20px; border: 1px solid #007bff;">
📊 <b>Compliance Ready:</b>



Designed to meet <b>SEBI BRSR Core</b> requirements for "Reasonable Assurance" in the Value Chain.
</div>

<h2 id="overview" style="font-size:2.8em;">🏢 Overview</h2>
<p style="font-size:1.3em;">
<strong>CarbonLens AI</strong> is a sophisticated ESG governance platform designed to solve the "Scope 3 Nightmare." While companies can easily track their own emissions, 90% of their footprint is hidden in unstructured supplier invoices and utility bills.
</p>
<p style="font-size:1.3em;">
Built with <b>Flask</b> and <b>Llama 3.3 (Groq)</b>, CarbonLens AI bridges the data gap by allowing suppliers to upload raw evidence, which the AI then parses, normalizes, and presents to company staff for human-in-the-loop verification.
</p>

<h2 id="features" style="font-size:2.8em;">🛠 Key Features</h2>
<ul style="font-size:1.3em;">
<li>🔐 <b>Dual-Portal Architecture</b> — Separate secure dashboards for Corporate Staff and Suppliers using RBAC.</li>
<li>🤖 <b>AI Evidence Extraction</b> — Automated parsing of PDF/Excel invoices powered by <b>Groq Llama 3.3</b>.</li>
<li>⚖️ <b>Governance Queue</b> — A "Human-in-the-loop" approval system where staff verify AI reasoning before data hits the ledger.</li>
<li>📏 <b>Deterministic Math</b> — All extractions are mapped to a standardized <code>emission_factors.csv</code> for scientific accuracy.</li>
<li>📜 <b>SEBI BRSR Reporting</b> — One-click generation of audit-ready PDFs containing the full chain of custody for every metric.</li>
<li>🚨 <b>Anomalous Emission Alerts</b> — Automatic SMTP triggers when a supplier’s carbon intensity exceeds predefined thresholds.</li>
<li>☁️ <b>Firebase Integration</b> — Real-time data synchronization and secure document storage.</li>
</ul>

<h2 id="workflow" style="font-size:2.8em;">⚙️ Technical Workflow</h2>
<div style="background:#f9f9f9; padding:20px; border-radius:10px; border-left: 8px solid #007bff; font-size:1.2em;">
1️⃣ <b>Supplier Upload:</b> Raw invoice (PDF/XLSX) is uploaded via the Supplier Portal.



2️⃣ <b>AI Analysis:</b> NLP extracts activity type (Diesel, Electricity, Steel) and quantity.



3️⃣ <b>Normalization:</b> Data is converted to standardized units using a scientific factor database.



4️⃣ <b>Verification:</b> Staff reviews the "AI Explanation" and evidence before clicking <b>Approve</b>.



5️⃣ <b>Aggregation:</b> Approved data updates the Global Scope 3 Dashboard and Compliance Reports.
</div>

<h2 id="installation" style="font-size:2.8em;">🚀 Installation</h2>
<h3 style="font-size:2em;">Prerequisites</h3>
<ul style="font-size:1.3em;">
<li>Python 3.10+</li>
<li>Firebase Project (Firestore + Storage)</li>
<li>Groq API Key (for Llama 3.3)</li>
<li>SMTP App Password (for automated alerts)</li>
</ul>

<h3 style="font-size:2em;">Setup Guide</h3>
<div style="background:#2d2d2d; color:#cccccc; padding:20px; border-radius:10px; font-size:1.3em; overflow-x:auto;">

<b>1️⃣ Clone & Navigate:</b>

<pre style="color:#fff;">git clone https://github.com/your-username/carbonlens-ai.git
cd carbonlens-ai</pre>

<b>2️⃣ Install Requirements:</b>

<pre style="color:#fff;">pip install -r requirements.txt</pre>

<b>3️⃣ Configuration:</b>

<ul>
<li>Place <code>firebase_key.json</code> in the <code>static/</code> folder.</li>
<li>Ensure <code>emission_factors.csv</code> is in the root directory.</li>
<li>Update <code>app.py</code> with your <b>GROQ_API_KEY</b> and <b>SMTP</b> credentials.</li>
</ul>

<b>4️⃣ Launch Platform:</b>

<pre style="color:#fff;">python app.py</pre>

</div>

<h2 id="usage" style="font-size:2.8em;">📋 Usage Roles</h2>
<table style="width:100%; font-size:1.2em; border-collapse: collapse; margin-top:20px;">
<tr style="background-color: #007bff; color: white;">
<th style="padding: 15px; border: 1px solid #ddd;">Role</th>
<th style="padding: 15px; border: 1px solid #ddd;">Access Code</th>
<th style="padding: 15px; border: 1px solid #ddd;">Primary Function</th>
</tr>
<tr>
<td style="padding: 15px; border: 1px solid #ddd;"><b>Company Staff</b></td>
<td style="padding: 15px; border: 1px solid #ddd;"><code>CORP2026</code></td>
<td style="padding: 15px; border: 1px solid #ddd;">Verify evidence, manage portfolio, & export BRSR reports.</td>
</tr>
<tr>
<td style="padding: 15px; border: 1px solid #ddd;"><b>Supplier</b></td>
<td style="padding: 15px; border: 1px solid #ddd;"><code>SUPP7710</code></td>
<td style="padding: 15px; border: 1px solid #ddd;">Upload invoices, view personal footprint, & receive alerts.</td>
</tr>
</table>

<div align="center">
<p style="font-size:1.1em; color:#666;">
Developed for the 2026 Sustainability Hackathon 



<b>Empowering Green Supply Chains with Artificial Intelligence</b>
</p>
</div>
