// The view's one script: every button and form calls the API with the
// page's CSRF token; the live parts ask the API again; nothing it shows is
// written as markup, only as text.
(function () {
  "use strict";
  var meta = document.querySelector('meta[name="csrf-token"]');
  var csrf = meta ? meta.getAttribute("content") : "";

  function say(text) {
    var el = document.getElementById("said");
    if (!el) { return; }
    el.textContent = text;
    el.hidden = !text;
    if (text) { el.scrollIntoView({block: "nearest"}); }
  }

  function call(method, url, body) {
    return fetch(url, {
      method: method, credentials: "same-origin",
      headers: {"Content-Type": "application/json", "X-CSRF-Token": csrf},
      body: body === undefined ? undefined : JSON.stringify(body)
    }).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (data) {
        if (!r.ok) { throw new Error(data.error || ("refused (" + r.status + ")")); }
        return data;
      });
    });
  }

  function after(el, data) {
    var hop = el.getAttribute("data-go-run");
    if (hop && data && data.id) {
      location.href = "/hops/" + encodeURIComponent(hop) + "/runs/" + encodeURIComponent(data.id);
      return;
    }
    if (el.hasAttribute("data-show-token") && data && data.token) {
      var box = document.querySelector("[data-token]");
      box.textContent = "Your token, shown this once: " + data.token;
      box.hidden = false;
      return;
    }
    location.reload();
  }

  document.addEventListener("click", function (ev) {
    var el = ev.target.closest("[data-post]");
    if (!el) { return; }
    ev.preventDefault();
    var ask = el.getAttribute("data-confirm");
    if (ask && !window.confirm(ask)) { return; }
    var body = {};
    try { body = JSON.parse(el.getAttribute("data-body") || "{}"); } catch (e) { body = {}; }
    el.disabled = true;
    call(el.getAttribute("data-method") || "POST", el.getAttribute("data-post"), body)
      .then(function (data) { after(el, data); })
      .catch(function (e) { say(e.message); el.disabled = false; });
  });

  document.addEventListener("submit", function (ev) {
    var form = ev.target;
    if (!form.hasAttribute("data-api")) { return; }
    ev.preventDefault();
    var button = form.querySelector("[data-confirm]");
    if (button && !window.confirm(button.getAttribute("data-confirm"))) { return; }
    var body = {};
    Array.prototype.forEach.call(form.elements, function (f) {
      if (!f.name) { return; }
      if (f.type === "checkbox") { if (f.checked) { body[f.name] = true; } return; }
      if (f.value === "") { return; }
      body[f.name] = f.type === "number" ? Number(f.value) : f.value;
    });
    call("POST", form.getAttribute("data-api"), body)
      .then(function (data) { after(form, data); })
      .catch(function (e) { say(e.message); });
  });

  // the live progress: the same answer the API gives, laid out as text
  function cell(row, text) {
    var td = document.createElement("td");
    td.textContent = text;
    row.appendChild(td);
  }
  function took(s) {
    if (s === null || s === undefined) { return ""; }
    if (s >= 3600) { return (s / 3600).toFixed(1) + " hours"; }
    if (s >= 60) { return Math.round(s / 60) + " minutes"; }
    return Math.max(1, Math.round(s)) + " seconds";
  }
  function progress(el, data) {
    var body = el.querySelector('[data-slot="databases"] tbody');
    if (body) {
      var keep = body.querySelectorAll("button").length > 0;
      if (!keep) {
        body.textContent = "";
        (data.databases || []).forEach(function (d) {
          var tr = document.createElement("tr");
          cell(tr, d.db);
          var m = d.move;
          cell(tr, m ? ((m.rows_done || 0).toLocaleString() + (m.rows_total ? " of " + m.rows_total.toLocaleString() : "") + " rows" + (m.done ? ", done" : (m.eta_copy ? ", about " + took(m.eta_copy) + " left" : ""))) : "-");
          var t = d.tail;
          cell(tr, t ? (t.running ? (t.paused ? "held" : "running") + (t.behind !== null && t.behind !== undefined ? ", " + took(t.behind) + " behind" : "") : "stopped") : "-");
          cell(tr, "");
          body.appendChild(tr);
        });
      }
    }
    var v = el.querySelector('[data-slot="verdict"] .pill');
    if (v && data.verdict) { v.textContent = data.verdict.status; v.className = "pill " + data.verdict.status; }
  }
  document.querySelectorAll("[data-live]").forEach(function (el) {
    var every = Number(el.getAttribute("data-every") || 10) * 1000;
    setInterval(function () {
      call("GET", el.getAttribute("data-live")).then(function (d) { progress(el, d); }).catch(function () {});
    }, every);
  });

  // a run's output as it is written
  document.querySelectorAll("[data-output]").forEach(function (pre) {
    if (pre.getAttribute("data-running") !== "1") { return; }
    var offset = Number(pre.getAttribute("data-offset") || 0);
    function more() {
      call("GET", pre.getAttribute("data-output") + "?offset=" + offset).then(function (d) {
        if (d.text) { pre.appendChild(document.createTextNode(d.text)); pre.scrollTop = pre.scrollHeight; }
        offset = d.offset;
        var st = document.querySelector("[data-state]");
        if (st) { st.textContent = d.state; st.className = "pill " + d.state; }
        if (d.state === "queued" || d.state === "running") { setTimeout(more, 2000); }
        else { setTimeout(function () { location.reload(); }, 1000); }
      }).catch(function () { setTimeout(more, 5000); });
    }
    setTimeout(more, 1500);
  });

  // the set-up address carries its code after '#', which no browser sends
  var setup = document.querySelector("form[data-setup] input[name=code]");
  if (setup && !setup.value && location.hash.length > 1) {
    setup.value = decodeURIComponent(location.hash.slice(1));
    history.replaceState(null, "", location.pathname);
  }
})();
