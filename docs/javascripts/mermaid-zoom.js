/* Pan/zoom for Mermaid diagrams (svg-pan-zoom loaded via CDN).
   Material renders mermaid asynchronously, so watch for SVGs appearing. */
(function () {
  "use strict";

  function enhance(svg) {
    if (svg.dataset.panZoom || typeof svgPanZoom === "undefined") return;
    svg.dataset.panZoom = "1";
    svg.style.maxWidth = "none";
    svg.removeAttribute("width");
    svgPanZoom(svg, {
      zoomEnabled: true,
      controlIconsEnabled: true,
      fit: true,
      center: true,
      minZoom: 0.2,
      maxZoom: 10,
      dblClickZoomEnabled: true,
    });
  }

  function scan(root) {
    root.querySelectorAll(".mermaid svg").forEach(enhance);
  }

  function watch() {
    document.querySelectorAll(".mermaid").forEach(function (el) {
      if (el.dataset.zoomWatch) return;
      el.dataset.zoomWatch = "1";
      new MutationObserver(function () {
        scan(el.parentElement || el);
      }).observe(el, { childList: true, subtree: true });
    });
    scan(document);
  }

  if (typeof document$ !== "undefined") {
    // Material's instant navigation observable
    document$.subscribe(watch);
  } else {
    document.addEventListener("DOMContentLoaded", watch);
  }
})();
