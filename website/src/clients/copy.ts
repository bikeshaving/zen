// Progressive enhancement: attach a "Copy" button to every server-rendered
// code block. No framework needed — the markup is static, we just decorate it.

function attachCopyButtons() {
	const containers = document.querySelectorAll<HTMLElement>(
		".code-block-container",
	);
	for (const container of Array.from(containers)) {
		if (container.querySelector(".copy-button")) continue;
		const code = container.querySelector("code");
		if (!code) continue;

		const button = document.createElement("button");
		button.type = "button";
		button.className = "copy-button";
		button.textContent = "Copy";
		button.setAttribute("aria-label", "Copy code to clipboard");

		let resetTimer: ReturnType<typeof setTimeout> | null = null;
		button.addEventListener("click", async () => {
			const text = code.textContent ?? "";
			try {
				await navigator.clipboard.writeText(text);
				button.textContent = "Copied!";
				button.classList.add("copied");
			} catch {
				button.textContent = "Failed";
			}
			if (resetTimer) clearTimeout(resetTimer);
			resetTimer = setTimeout(() => {
				button.textContent = "Copy";
				button.classList.remove("copied");
			}, 2000);
		});

		container.appendChild(button);
	}
}

attachCopyButtons();
