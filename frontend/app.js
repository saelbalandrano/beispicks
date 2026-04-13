// Matriz de Colores Oficiales MLB
const mlbColors = {
    108: "#F9A01B", 109: "#A05500", 110: "#000000", 111: "#002D72", 
    112: "#002B5C", 113: "#CE1141", 114: "#004687", 115: "#33006F", 
    116: "#0C2340", 117: "#E31937", 118: "#004687", 119: "#002D72", 
    120: "#003831", 121: "#005A9C", 133: "#00A3E0", 134: "#002868", 
    135: "#002B5C", 136: "#002D72", 137: "#FD5A1E", 138: "#C41E3A", 
    139: "#00A1DE", 140: "#002A5C", 141: "#00417D", 142: "#8FBCE6", 
    143: "#E31937", 144: "#002D72", 145: "#003263", 146: "#0C2340", 
    147: "#002D72", 158: "#12284B", 103: "#E31937"
};

const defaultColor = "#555555";
const logoBase = "https://www.mlbstatic.com/team-logos/";

// Cargar la proyección generada por main.py
async function loadPicks() {
    try {
        // En producción GitHub pages servirá este archivo
        const response = await fetch('./data/picks.json');
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const picks = await response.json();
        renderDashboard(picks);
    } catch (err) {
        console.error("Error cargando JSON de inteligencia", err);
        document.getElementById('approved-picks-container').innerHTML = `
            <div style="grid-column: 1/-1; padding: 2rem; border-radius: 10px; background: rgba(255,0,0,0.1); border: 1px dashed red;">
                <h3 style="color:red">Sindicato Desconectado</h3>
                <p style="color:#aaa; margin-top:5px;">Asegúrate de correr <code>python main.py</code> primero para que genere el archivo <code>picks.json</code>.</p>
            </div>
        `;
    }
}

function renderDashboard(picks) {
    const approvedGrid = document.getElementById('approved-picks-container');
    const ignoredGrid = document.getElementById('ignored-picks-container');

    approvedGrid.innerHTML = '';
    ignoredGrid.innerHTML = '';

    const approvedPicks = picks.filter(p => p.status === 'APROBADO' || p.ats_status === 'APROBADO');
    const ignoredPicks = picks.filter(p => p.status !== 'APROBADO' && p.ats_status !== 'APROBADO');

    if(approvedPicks.length === 0) {
        approvedGrid.innerHTML = `
            <div style="grid-column: 1/-1; padding: 3rem; text-align: center; background: rgba(255,255,255,0.02); border-radius: 15px; border: 1px dashed rgba(255,255,255,0.1)">
                <span style="font-size:2rem; opacity:0.5">---</span>
                <h3 style="margin-top:1rem; color:var(--text-secondary)">No hay picks aprobados hoy.</h3>
                <p style="color:#666; font-size:0.9rem">El algoritmo no encontro bordes matematicos seguros.</p>
            </div>`;
    } else {
        approvedPicks.forEach(p => {
            const isAnyApproved = (p.status === 'APROBADO' || p.ats_status === 'APROBADO');
            approvedGrid.appendChild(createCard(p, isAnyApproved));
        });
    }

    ignoredPicks.forEach(p => ignoredGrid.appendChild(createCard(p, false)));
}

function getDisplayOdds(americanOdds) {
    if (americanOdds === 0) return 'TBA';
    return americanOdds > 0 ? `+${americanOdds}` : `${americanOdds}`;
}

function createCard(pick, isApproved) {
    const card = document.createElement('div');
    card.className = `card ${isApproved ? 'approved' : 'ignored'}`;

    const homeColor = mlbColors[pick.home_id] || defaultColor;
    const awayColor = mlbColors[pick.away_id] || defaultColor;

    // Pintar gradientes en fondo simulando luces de estadio 
    card.style.backgroundImage = `
        radial-gradient(circle at bottom right, ${homeColor}22, transparent 50%),
        radial-gradient(circle at top left, ${awayColor}15, transparent 40%)
    `;

    const hProb = pick.home_prob.toFixed(1);
    const aProb = pick.away_prob.toFixed(1);

    const hasTentative = pick.tentative_pick && pick.tentative_pick !== null;

    const mlApproved = pick.status === 'APROBADO';
    const atsApproved = pick.ats_status === 'APROBADO';
    const hasAts = pick.ats_status && pick.ats_status !== 'SIN DATOS';

    // MoneyLine line
    let mlLine = '';
    if (mlApproved) {
        const mlTeam = pick.tentative_pick === 'HOME' ? (pick.home_team_name || 'HOME') : (pick.away_team_name || 'AWAY');
        mlLine = `<div style="display:flex; justify-content:space-between; align-items:center; padding:6px 10px; border-radius:6px; background:rgba(160,255,46,0.12); border:1px solid #a0ff2e; margin-bottom:6px;">
            <span style="color:#a0ff2e; font-weight:800; font-size:0.75rem; letter-spacing:1px;">MONEYLINE</span>
            <span style="color:#fff; font-weight:700; font-size:0.9rem;">${mlTeam} ${getDisplayOdds(pick.odds)}</span>
        </div>`;
    } else if (hasTentative) {
        const mlTeam = pick.tentative_pick === 'HOME' ? (pick.home_team_name || 'HOME') : (pick.away_team_name || 'AWAY');
        mlLine = `<div style="display:flex; justify-content:space-between; align-items:center; padding:6px 10px; border-radius:6px; background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.1); margin-bottom:6px;">
            <span style="color:#888; font-weight:700; font-size:0.75rem; letter-spacing:1px;">MONEYLINE</span>
            <span style="color:#aaa; font-weight:600; font-size:0.85rem;">${mlTeam} ${getDisplayOdds(pick.odds)}</span>
        </div>`;
    } else {
        mlLine = `<div style="display:flex; justify-content:space-between; align-items:center; padding:6px 10px; border-radius:6px; background:rgba(255,255,255,0.02); border:1px solid rgba(255,255,255,0.05); margin-bottom:6px;">
            <span style="color:#555; font-weight:700; font-size:0.75rem; letter-spacing:1px;">MONEYLINE</span>
            <span style="color:#555; font-size:0.8rem;">---</span>
        </div>`;
    }

    // ATS line
    let atsLine = '';
    if (hasAts) {
        if (atsApproved) {
            const atsTeam = pick.ats_pick === 'HOME' ? (pick.home_team_name || 'HOME') : (pick.away_team_name || 'AWAY');
            const atsPoint = pick.ats_pick === 'HOME' ? pick.ats_home_point : pick.ats_away_point;
            const atsOdds = pick.ats_pick === 'HOME' ? pick.ats_home_odds : pick.ats_away_odds;
            atsLine = `<div style="display:flex; justify-content:space-between; align-items:center; padding:6px 10px; border-radius:6px; background:rgba(160,255,46,0.12); border:1px solid #a0ff2e;">
                <span style="color:#a0ff2e; font-weight:800; font-size:0.75rem; letter-spacing:1px;">ATS</span>
                <span style="color:#fff; font-weight:700; font-size:0.9rem;">${atsTeam} ${atsPoint > 0 ? '+' : ''}${atsPoint} (${getDisplayOdds(atsOdds)})</span>
            </div>`;
        } else {
            atsLine = `<div style="display:flex; justify-content:space-between; align-items:center; padding:6px 10px; border-radius:6px; background:rgba(255,255,255,0.02); border:1px solid rgba(255,255,255,0.05);">
                <span style="color:#555; font-weight:700; font-size:0.75rem; letter-spacing:1px;">ATS</span>
                <span style="color:#555; font-size:0.8rem;">---</span>
            </div>`;
        }
    }

    const actionHtml = `
        <div style="margin-top:12px;">
            ${mlLine}
            ${atsLine}
        </div>
    `;

    const probHtml = `
        <div class="prob-container">
            <div class="prob-label">
                <span style="color:#ffffff">${aProb}%</span>
                <span style="color:#ff3333">${hProb}%</span>
            </div>
            <div class="bar-bg">
                <div class="bar-fill" style="width: ${aProb}%; background: #ffffff; box-shadow: 0 0 10px rgba(255,255,255,0.8);"></div>
                <div class="bar-fill" style="width: ${hProb}%; background: #ff3333; box-shadow: 0 0 10px rgba(255,51,51,0.8);"></div>
            </div>
        </div>
    `;

    card.innerHTML = `
        <div class="match-header">
            <span>${new Date(pick.game_date + 'T12:00:00').toLocaleDateString('en-US', {month: 'short', day: 'numeric', year: 'numeric'})}</span>
            <span style="color: ${isApproved ? '#a0ff2e' : 'inherit'}">${isApproved ? 'ACTION' : 'INFO'}</span>
        </div>
        <div class="matchup-row">
            <div class="team-block" style="display: flex; flex-direction: column; align-items: center; justify-content: center;">
                <img src="${logoBase}${pick.away_id}.svg" class="team-logo" alt="Away" style="margin-bottom: 5px;">
                <span class="team-name" style="color: #ffffff; font-weight: 800; font-size: 0.85rem; text-align: center;">${pick.away_team_name || 'Away Team'}</span>
                <span style="color: rgba(255, 255, 255, 0.5); font-size: 0.65rem; margin-top: 4px; font-weight: bold;">AWAY</span>
            </div>
            <div class="vs">@</div>
            <div class="team-block" style="display: flex; flex-direction: column; align-items: center; justify-content: center;">
                <img src="${logoBase}${pick.home_id}.svg" class="team-logo" alt="Home" style="margin-bottom: 5px;">
                <span class="team-name" style="color: #ffffff; font-weight: 800; font-size: 0.85rem; text-align: center;">${pick.home_team_name || 'Home Team'}</span>
                <span style="color: rgba(255, 255, 255, 0.5); font-size: 0.65rem; margin-top: 4px; font-weight: bold;">HOME</span>
            </div>
        </div>
        ${probHtml}
        ${actionHtml}
    `;

    return card;
}

// Iniciar aplicación
document.addEventListener('DOMContentLoaded', () => {
    loadPicks();
    setupTabs();
});

function setupTabs() {
    const tabToday = document.getElementById('tab-today');
    const tabHistory = document.getElementById('tab-history');
    const viewToday = document.getElementById('view-today');
    const viewHistory = document.getElementById('view-history');
    const yearSelect = document.getElementById('ledger-year');
    const marketSelect = document.getElementById('ledger-market');

    tabToday.addEventListener('click', () => {
        tabToday.classList.add('active');
        tabHistory.classList.remove('active');
        viewToday.style.display = 'block';
        viewHistory.style.display = 'none';
    });

    tabHistory.addEventListener('click', () => {
        tabHistory.classList.add('active');
        tabToday.classList.remove('active');
        viewHistory.style.display = 'block';
        viewToday.style.display = 'none';
        loadLedger(yearSelect.value, marketSelect.value);
    });

    yearSelect.addEventListener('change', () => {
        loadLedger(yearSelect.value, marketSelect.value);
    });
    marketSelect.addEventListener('change', () => {
        loadLedger(yearSelect.value, marketSelect.value);
    });
}

const marketLabels = {
    'h2h': 'Moneyline',
    'spreads': 'ATS',
    'totals': 'Over/Under',
    'h2h_1st_5_innings': 'F5 ML',
    'spreads_1st_5_innings': 'F5 Spread',
    'totals_1st_5_innings': 'F5 O/U'
};

function getMarketLabel(key) {
    return marketLabels[key] || key;
}

function calcStats(rows) {
    let w = 0, l = 0, p = 0, profit = 0;
    rows.forEach(r => {
        if (r.status === 'WON') w++;
        else if (r.status === 'LOST') l++;
        else if (r.status === 'PUSH') p++;
        profit += parseFloat(r.profit_loss || 0);
    });
    const wr = (w + l) > 0 ? (w / (w + l) * 100).toFixed(1) : '0.0';
    return { w, l, p, profit, wr };
}

async function loadLedger(year, marketFilter) {
    try {
        const response = await fetch('data/ledger.json');
        const data = await response.json();
        const tbody = document.getElementById('ledger-tbody');
        tbody.innerHTML = '';

        const yearData = data.filter(d => d.game_date.startsWith(year));
        yearData.sort((a, b) => new Date(b.game_date) - new Date(a.game_date));

        // --- Auto-detect markets and populate filter ---
        const marketsFound = [...new Set(yearData.map(r => r.market_type).filter(Boolean))];
        const marketSelect = document.getElementById('ledger-market');
        const currentVal = marketSelect.value;
        // Keep ALL option, rebuild the rest
        marketSelect.innerHTML = '<option value="ALL">All Markets</option>';
        marketsFound.forEach(mk => {
            const opt = document.createElement('option');
            opt.value = mk;
            opt.textContent = getMarketLabel(mk);
            marketSelect.appendChild(opt);
        });
        marketSelect.value = currentVal; // preserve selection

        // --- Filter by market if not ALL ---
        const filtered = (!marketFilter || marketFilter === 'ALL')
            ? yearData
            : yearData.filter(r => r.market_type === marketFilter);

        // --- Total stats (all markets) ---
        const totals = calcStats(yearData);
        document.getElementById('ledger-wlp').innerText = `${totals.w}-${totals.l}-${totals.p}`;
        document.getElementById('ledger-winrate').innerText = `${totals.wr}%`;
        const profitEl = document.getElementById('ledger-profit');
        profitEl.innerText = `${totals.profit >= 0 ? '+' : ''}$${totals.profit.toFixed(2)}`;
        profitEl.style.color = totals.profit >= 0 ? '#a0ff2e' : '#ff4757';

        // --- Per-market breakdown cards ---
        const breakdownEl = document.getElementById('market-breakdown');
        breakdownEl.innerHTML = '';
        marketsFound.forEach(mk => {
            const mkRows = yearData.filter(r => r.market_type === mk);
            const s = calcStats(mkRows);
            const label = getMarketLabel(mk);
            const profitColor = s.profit >= 0 ? '#a0ff2e' : '#ff4757';
            const card = document.createElement('div');
            card.className = 'bankroll-card';
            card.style.cssText = 'flex:1; min-width:200px;';
            card.innerHTML = `
                <h3 style="color:${profitColor}; font-size:0.75rem;">${label}</h3>
                <h2 style="font-size:1.5rem; color:${profitColor};">${s.profit >= 0 ? '+' : ''}$${s.profit.toFixed(2)}</h2>
                <div style="margin-top:8px; font-size:0.8rem; color:#888;">
                    ${s.w}-${s.l}-${s.p} &nbsp;|&nbsp; ${s.wr}%
                </div>
            `;
            breakdownEl.appendChild(card);
        });

        // --- Render table rows ---
        filtered.forEach(row => {
            let statusClass = '';
            if (row.status === 'WON') statusClass = 'status-won';
            if (row.status === 'LOST') statusClass = 'status-lost';
            if (row.status === 'PUSH') statusClass = 'status-push';

            const tr = document.createElement('tr');
            const homeName = row.home_team_name || '?';
            const awayName = row.away_team_name || '?';
            const matchup = `${awayName} @ ${homeName}`;
            const pickName = row.pick_team === 'HOME' ? homeName : awayName;
            const mLabel = getMarketLabel(row.market_type);

            tr.innerHTML = `
                <td>${row.game_date}</td>
                <td style="font-size:0.85rem;">${matchup}</td>
                <td style="font-weight:bold; color:#fff;">${pickName}</td>
                <td><span style="background:rgba(255,255,255,0.1); padding:2px 6px; border-radius:4px;">${mLabel}</span></td>
                <td>${row.odds > 0 ? '+' : ''}${row.odds}</td>
                <td style="color:#feca57; font-weight:600;">$${parseFloat(row.stake || 100).toFixed(0)}</td>
                <td class="${statusClass}">${row.status}</td>
                <td style="color:${parseFloat(row.profit_loss) > 0 ? '#a0ff2e' : (parseFloat(row.profit_loss) < 0 ? '#ff4757' : '#fff')}; font-weight:bold;">
                    ${parseFloat(row.profit_loss) > 0 ? '+' : ''}$${parseFloat(row.profit_loss).toFixed(2)}
                </td>
            `;
            tbody.appendChild(tr);
        });

        if (filtered.length === 0) {
            tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;">No picks recorded for this filter.</td></tr>';
        }

    } catch (error) {
        console.log("No ledger data found: ", error);
        document.getElementById('ledger-tbody').innerHTML = '<tr><td colspan="8" style="text-align:center; padding: 2rem;">Awaiting daily sync.</td></tr>';
    }
}

