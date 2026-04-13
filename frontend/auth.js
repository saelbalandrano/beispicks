// ============================================================
//  SUPABASE AUTH CONFIG
//  La anon key es segura para exponer en el frontend.
//  La URL y anon key se configuran aqui para autenticación.
//  IMPORTANTE: Este archivo NO contiene la service_role key.
// ============================================================
const SUPABASE_URL = 'https://qoiuwnjhtztkrjpdvlsc.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFvaXV3bmpodHp0a3JqcGR2bHNjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU2OTA1MDIsImV4cCI6MjA5MTI2NjUwMn0.HECB1CRA1tE-64Q1kC9s_pFgIW5Mek_74xds0JUShr4';

const _supabase = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

// ============================================================
//  AUTH FUNCTIONS
// ============================================================
async function signIn(email, password) {
    const { data, error } = await _supabase.auth.signInWithPassword({
        email: email,
        password: password
    });
    if (error) throw error;
    return data;
}

async function signOut() {
    await _supabase.auth.signOut();
    showLogin();
}

async function getSession() {
    const { data: { session } } = await _supabase.auth.getSession();
    return session;
}

// ============================================================
//  UI GATE: Login vs App
// ============================================================
function showLogin() {
    document.getElementById('login-screen').style.display = 'flex';
    document.getElementById('app-container').style.display = 'none';
}

function showApp() {
    document.getElementById('login-screen').style.display = 'none';
    document.getElementById('app-container').style.display = 'block';
}

async function checkAuth() {
    const session = await getSession();
    if (session) {
        showApp();
        const userEmail = session.user.email;
        const userEl = document.getElementById('user-email');
        if (userEl) userEl.textContent = userEmail;
        return true;
    } else {
        showLogin();
        return false;
    }
}

// Login form handler
function setupAuthForm() {
    const form = document.getElementById('login-form');
    const errorEl = document.getElementById('login-error');

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        errorEl.textContent = '';
        const email = document.getElementById('login-email').value;
        const password = document.getElementById('login-password').value;
        
        const btn = form.querySelector('button');
        btn.disabled = true;
        btn.textContent = 'Authenticating...';

        try {
            await signIn(email, password);
            showApp();
            loadPicks();
        } catch (err) {
            errorEl.textContent = err.message || 'Authentication failed';
        } finally {
            btn.disabled = false;
            btn.textContent = 'ACCESS';
        }
    });
}

// Listen for auth state changes
function setupAuthListener() {
    _supabase.auth.onAuthStateChange((event, session) => {
        if (event === 'SIGNED_OUT') {
            showLogin();
        }
    });
}
