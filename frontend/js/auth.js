// Authentication module
const API_URL = 'http://localhost:8000';

const Auth = {
    token: null,
    user: null,

    init() {
        // Check if user is already logged in
        const token = localStorage.getItem('token');
        const user = localStorage.getItem('user');
        
        if (token && user) {
            try {
                this.token = token;
                this.user = JSON.parse(user);
                
                // Basic token validation (check if it's a valid JWT format)
                const tokenParts = token.split('.');
                if (tokenParts.length !== 3) {
                    console.error("Invalid token format");
                    this.logout();
                    return false;
                }
                
                return true;
            } catch (error) {
                console.error("Error parsing user data", error);
                this.logout();
                return false;
            }
        }
        return false;
    },

    async login(email, password) {
        try {
            const response = await fetch(`${API_URL}/auth/login/json`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ email, password })
            });
            
            const data = await response.json();
            
            if (response.ok && data.access_token) {
                this.token = data.access_token;
                this.user = data.user;
                
                localStorage.setItem('token', this.token);
                localStorage.setItem('user', JSON.stringify(this.user));
                return { success: true };
            } else {
                return { success: false, message: data.detail || 'Login failed' };
            }
        } catch (error) {
            console.error("Login error:", error);
            return { success: false, message: 'Network error. Please try again.' };
        }
    },

    async register(username, email, password) {
        try {
            const response = await fetch(`${API_URL}/auth/register`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ username, email, password })
            });
            
            const data = await response.json();
            
            if (response.ok) {
                return { success: true };
            } else {
                return { success: false, message: data.detail || 'Registration failed' };
            }
        } catch (error) {
            return { success: false, message: 'Network error. Please try again.' };
        }
    },

    logout() {
        this.token = null;
        this.user = null;
        localStorage.removeItem('token');
        localStorage.removeItem('user');
    },

    getAuthHeaders() {
        return {
            'Authorization': `Bearer ${this.token}`,
            'Content-Type': 'application/json'
        };
    }
};
