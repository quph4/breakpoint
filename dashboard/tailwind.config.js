/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#0e0f12",
        paper: "#f6f3ec",
        court: "#3d6b4e",
        clay: "#b25c2c",
        ace: "#d4a72c",
      },
      fontFamily: {
        serif: ["Georgia", "Cambria", "serif"],
        mono: ["ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
    },
  },
  plugins: [],
};
