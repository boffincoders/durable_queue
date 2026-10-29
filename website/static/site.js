// Small progressive enhancements. Every page works without this script.
(() => {
  const root = document.querySelector('meta[name="dq-root"]')?.content || "./";
  const store = {
    get: (k) => { try { return localStorage.getItem(k); } catch { return null; } },
    set: (k, v) => { try { localStorage.setItem(k, v); } catch { /* storage unavailable */ } },
  };

  // Theme: follows the system until the visitor picks one.
  const saved = store.get("dq-theme");
  if (saved) document.documentElement.dataset.theme = saved;
  document.querySelector("[data-theme-toggle]")?.addEventListener("click", () => {
    const dark = document.documentElement.dataset.theme
      ? document.documentElement.dataset.theme === "dark"
      : matchMedia("(prefers-color-scheme: dark)").matches;
    const next = dark ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    store.set("dq-theme", next);
  });

  // Mobile navigation.
  document.querySelector("[data-menu]")?.addEventListener("click", (e) => {
    const open = document.body.classList.toggle("nav-open");
    e.currentTarget.setAttribute("aria-expanded", String(open));
  });

  // Copy buttons on code blocks and the install line.
  const copy = async (text, button) => {
    try {
      await navigator.clipboard.writeText(text);
      const label = button.textContent;
      button.textContent = "Copied";
      setTimeout(() => (button.textContent = label), 1200);
    } catch { /* clipboard blocked */ }
  };
  document.querySelectorAll("pre").forEach((pre) => {
    const button = document.createElement("button");
    button.className = "copy";
    button.type = "button";
    button.textContent = "Copy";
    button.addEventListener("click", () => copy(pre.querySelector("code")?.innerText ?? pre.innerText, button));
    pre.appendChild(button);
  });
  document.querySelectorAll("[data-copy]").forEach((button) =>
    button.addEventListener("click", () => copy(button.dataset.copy, button))
  );

  // Code tabs (Dart / Flutter / pubspec.yaml). The choice is remembered across pages.
  const preferred = store.get("dq-tab");
  document.querySelectorAll(".code-tabs").forEach((group) => {
    const tabs = [...group.querySelectorAll('[role="tab"]')];
    const select = (tab, remember) => {
      tabs.forEach((t) => {
        const on = t === tab;
        t.setAttribute("aria-selected", String(on));
        t.tabIndex = on ? 0 : -1;
        group.querySelector(`#${t.getAttribute("aria-controls")}`).hidden = !on;
      });
      if (remember) store.set("dq-tab", tab.dataset.kind);
    };
    tabs.forEach((tab, i) => {
      tab.addEventListener("click", () => select(tab, true));
      tab.addEventListener("keydown", (e) => {
        if (e.key !== "ArrowRight" && e.key !== "ArrowLeft") return;
        const next = tabs[(i + (e.key === "ArrowRight" ? 1 : tabs.length - 1)) % tabs.length];
        select(next, true);
        next.focus();
      });
    });
    const initial = tabs.find((t) => t.dataset.kind === preferred) || tabs[0];
    select(initial, false);
  });

  // Search (Ctrl/Cmd+K or "/").
  const dialog = document.querySelector("dialog.search");
  const input = dialog?.querySelector("input");
  const list = dialog?.querySelector("ul");
  const index = window.DQ_SEARCH || [];
  let active = 0;
  const render = () => {
    const q = input.value.trim().toLowerCase();
    const hits = (q ? index.filter((e) => (e.title + " " + e.text).toLowerCase().includes(q)) : index).slice(0, 12);
    active = Math.min(active, Math.max(hits.length - 1, 0));
    list.innerHTML = "";
    hits.forEach((hit, i) => {
      const li = document.createElement("li");
      const a = document.createElement("a");
      a.href = /^https?:/.test(hit.url) ? hit.url : root + hit.url;
      a.className = i === active ? "active" : "";
      a.textContent = hit.title;
      const small = document.createElement("small");
      small.textContent = hit.text;
      a.appendChild(small);
      li.appendChild(a);
      list.appendChild(li);
    });
    if (!hits.length) list.innerHTML = "<li><a>No results</a></li>";
  };
  const open = () => { if (!dialog) return; dialog.showModal(); input.value = ""; active = 0; render(); input.focus(); };
  document.querySelector("[data-search]")?.addEventListener("click", open);
  document.addEventListener("keydown", (e) => {
    const typing = /input|textarea|select/i.test(document.activeElement?.tagName || "");
    if ((e.key === "k" && (e.metaKey || e.ctrlKey)) || (e.key === "/" && !typing)) { e.preventDefault(); open(); }
  });
  input?.addEventListener("input", () => { active = 0; render(); });
  input?.addEventListener("keydown", (e) => {
    const links = [...list.querySelectorAll("a[href]")];
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      active = (active + (e.key === "ArrowDown" ? 1 : links.length - 1)) % Math.max(links.length, 1);
      links.forEach((l, i) => (l.className = i === active ? "active" : ""));
    } else if (e.key === "Enter" && links[active]) {
      location.href = links[active].href;
    }
  });
  dialog?.addEventListener("click", (e) => { if (e.target === dialog) dialog.close(); });
})();
