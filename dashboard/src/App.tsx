import { Link, NavLink, Route, Routes } from "react-router-dom";
import Home from "./pages/Home";
import Bets from "./pages/Bets";
import Players from "./pages/Players";
import Model from "./pages/Model";
import About from "./pages/About";

const navClass = ({ isActive }: { isActive: boolean }) =>
  `px-3 py-1.5 rounded-md text-sm transition ${
    isActive ? "bg-ink text-paper" : "text-ink/70 hover:text-ink"
  }`;

export default function App() {
  return (
    <div className="min-h-screen flex flex-col">
      <header className="border-b border-ink/10 bg-paper/80 backdrop-blur sticky top-0 z-10">
        <div className="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between">
          <Link to="/" className="font-serif text-2xl font-bold tracking-tight">
            breakpoint<span className="text-clay">.</span>
          </Link>
          <nav className="flex gap-1">
            <NavLink to="/" end className={navClass}>Home</NavLink>
            <NavLink to="/bets" className={navClass}>Bets</NavLink>
            <NavLink to="/players" className={navClass}>Players</NavLink>
            <NavLink to="/model" className={navClass}>Model</NavLink>
            <NavLink to="/about" className={navClass}>About</NavLink>
          </nav>
        </div>
      </header>

      <main className="flex-1 max-w-6xl w-full mx-auto px-6 py-8">
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/bets" element={<Bets />} />
          <Route path="/players" element={<Players />} />
          <Route path="/model" element={<Model />} />
          <Route path="/about" element={<About />} />
        </Routes>
      </main>

      <footer className="border-t border-ink/10 mt-auto">
        <div className="max-w-6xl mx-auto px-6 py-4 text-xs text-ink/50 flex justify-between">
          <span>fake money. real models.</span>
          <a href="https://github.com/quph4/breakpoint" className="hover:text-ink">
            github.com/quph4/breakpoint
          </a>
        </div>
      </footer>
    </div>
  );
}
