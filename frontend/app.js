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

    const approvedPicks = picks.filter(p => p.status === 'APROBADO');
    const ignoredPicks = picks.filter(p => p.status !== 'APROBADO');

    if(approvedPicks.length === 0) {
        approvedGrid.innerHTML = `
            <div style="grid-column: 1/-1; padding: 3rem; text-align: center; background: rgba(255,255,255,0.02); border-radius: 15px; border: 1px dashed rgba(255,255,255,0.1)">
                <span style="font-size:2rem; opacity:0.5">🛡️</span>
                <h3 style="margin-top:1rem; color:var(--text-secondary)">No hay picks aprobados hoy.</h3>
                <p style="color:#666; font-size:0.9rem">El algoritmo no encontró bordes matemáticos seguros.</p>
            </div>`;
    } else {
        approvedPicks.forEach(p => approvedGrid.appendChild(createCard(p, true)));
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

    const actionHtml = isApproved ? `
        <div class="action-area" style="text-align: center; border-radius: 8px; background: rgba(160,255,46,0.1); border: 1px solid #a0ff2e; padding: 12px; margin-top: 15px;">
            <div style="font-size: 0.70rem; color: #a0ff2e; text-transform: uppercase; font-weight: bold; letter-spacing: 1px;">🏆 Sindicato Pick Oficial</div>
            <div style="color: #ffffff; font-weight: 900; font-size: 1.25rem; margin-top: 6px;">${pick.tentative_pick === 'HOME' ? (pick.home_team_name || 'HOME') : (pick.away_team_name || 'AWAY')} <span>${getDisplayOdds(pick.odds)}</span></div>
            <div style="margin-top: 8px; font-size:0.75rem; color:rgba(255,255,255,0.7)">Confianza Vegas: ${pick.confianza}%</div>
        </div>
    ` : (hasTentative ? `
        <div class="action-area" style="text-align: center; border-radius: 8px; background: rgba(40,40,40,0.8); border: 1px dashed rgba(255,255,255,0.2); padding: 12px; margin-top: 15px;">
            <div style="font-size: 0.70rem; color: #aaaaaa; text-transform: uppercase;">Pick Sugerido (No Oficial)</div>
            <div style="color: #ffffff; font-weight: bold; font-size: 1rem; margin-top: 6px;">
                ${pick.tentative_pick === 'HOME' ? (pick.home_team_name || 'HOME') : (pick.away_team_name || 'AWAY')} (${pick.tentative_pick}) <span>${getDisplayOdds(pick.odds)}</span>
            </div>
        </div>
    ` : `
        <div class="action-area" style="text-align: center; border-radius: 8px; background: rgba(20,20,20,0.5); border: 1px solid rgba(255,255,255,0.05); padding: 10px; margin-top: 15px;">
            <div style="font-size: 0.9rem; color: #666666; font-weight: bold; letter-spacing: 2px;">🚫 NO PICK</div>
        </div>
    `);

    const probHtml = (isApproved || hasTentative) ? `
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
    ` : '';

    card.innerHTML = `
        <div class="match-header">
            <span>GAME ID: ${pick.game_pk}</span>
            <span style="color: ${isApproved ? '#a0ff2e' : 'inherit'}">${isApproved ? '⚡ ACTION' : 'INFO'}</span>
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
        loadLedger(yearSelect.value);
    });

    yearSelect.addEventListener('change', (e) => {
        loadLedger(e.target.value);
    });
}

async function loadLedger(year) {
    try {
        const response = await fetch('data/ledger.json');
        const data = await response.json();
        const tbody = document.getElementById('ledger-tbody');
        tbody.innerHTML = '';

        const yearData = data.filter(d => d.game_date.startsWith(year));
        yearData.sort((a, b) => new Date(b.game_date) - new Date(a.game_date));

        let wins = 0, losses = 0, pushes = 0;
        let totalProfit = 0.0;

        yearData.forEach(row => {
            if (row.status === 'WON') wins++;
            else if (row.status === 'LOST') losses++;
            else if (row.status === 'PUSH') pushes++;

            totalProfit += parseFloat(row.profit_loss || 0);

            let statusClass = '';
            if (row.status === 'WON') statusClass = 'status-won';
            if (row.status === 'LOST') statusClass = 'status-lost';
            if (row.status === 'PUSH') statusClass = 'status-push';

            const tr = document.createElement('tr');
            
            // Resolver nombre del matchup y pick
            const homeName = row.home_team_name || '?';
            const awayName = row.away_team_name || '?';
            const matchup = `${awayName} @ ${homeName}`;
            const pickName = row.pick_team === 'HOME' ? homeName : awayName;
            
            // Traducir market_type a nombres legibles
            const marketLabels = {
                'h2h': 'Moneyline',
                'spreads': 'Spread',
                'totals': 'Over/Under',
                'h2h_1st_5_innings': 'F5 Moneyline',
                'spreads_1st_5_innings': 'F5 Spread',
                'totals_1st_5_innings': 'F5 Over/Under'
            };
            const marketLabel = marketLabels[row.market_type] || row.market_type;
            
            tr.innerHTML = `
                <td>${row.game_date}</td>
                <td style="font-size:0.85rem;">${matchup}</td>
                <td style="font-weight:bold; color:#fff;">${pickName}</td>
                <td><span style="background:rgba(255,255,255,0.1); padding:2px 6px; border-radius:4px;">${marketLabel}</span></td>
                <td>${row.odds > 0 ? '+' : ''}${row.odds}</td>
                <td class="${statusClass}">${row.status}</td>
                <td style="color:${parseFloat(row.profit_loss) > 0 ? '#a0ff2e' : (parseFloat(row.profit_loss) < 0 ? '#ff4757' : '#fff')}; font-weight:bold;">
                    ${parseFloat(row.profit_loss) > 0 ? '+' : ''}$${parseFloat(row.profit_loss).toFixed(2)}
                </td>
            `;
            tbody.appendChild(tr);
        });

        if (yearData.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;">No official picks recorded for this period yet.</td></tr>';
        }

        document.getElementById('ledger-wlp').innerText = `${wins}-${losses}-${pushes}`;
        const winrate = (wins + losses) > 0 ? (wins / (wins + losses) * 100).toFixed(1) : 0.0;
        document.getElementById('ledger-winrate').innerText = `${winrate}%`;
        
        const profitEl = document.getElementById('ledger-profit');
        profitEl.innerText = `${totalProfit >= 0 ? '+' : ''}$${totalProfit.toFixed(2)}`;
        profitEl.style.color = totalProfit >= 0 ? '#a0ff2e' : '#ff4757';

    } catch (error) {
        console.log("No ledger data found yet or error loading: ", error);
        document.getElementById('ledger-tbody').innerHTML = '<tr><td colspan="7" style="text-align:center; padding: 2rem;">Awaiting daily sync. Missing ledger.json</td></tr>';
    }
}
