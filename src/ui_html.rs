pub(crate) const UI_HTML: &str = r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>raft</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f8fa;
      --panel: #ffffff;
      --panel-soft: #f0f6f4;
      --ink: #171a1f;
      --muted: #69727e;
      --line: #dce1e7;
      --accent: #0b6f6b;
      --accent-ink: #ffffff;
      --warn: #9a5a00;
      --warn-bg: #fff5de;
      --event: #5d4b9c;
      --error: #b23b3b;
      --shadow: 0 12px 30px rgba(23, 26, 31, 0.08);
    }

    * { box-sizing: border-box; }
    [hidden] { display: none !important; }
    body {
      margin: 0;
      height: 100vh;
      overflow: hidden;
      background: var(--bg);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: 0;
    }
    button, input, textarea, select { font: inherit; }
    button {
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      color: var(--ink);
      cursor: pointer;
      font-weight: 700;
    }
    button:hover { border-color: #a8b2bd; }
    button:focus-visible, input:focus-visible, textarea:focus-visible, select:focus-visible {
      outline: 3px solid rgba(11, 111, 107, 0.18);
      outline-offset: 1px;
    }
    input, textarea, select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      color: var(--ink);
      padding: 9px 10px;
      outline: 0;
    }
    textarea {
      resize: none;
      min-height: 52px;
      max-height: 160px;
      line-height: 1.45;
    }
    .app {
      display: grid;
      grid-template-columns: 320px minmax(0, 1fr);
      height: 100vh;
      min-width: 0;
    }
    .rooms {
      display: flex;
      flex-direction: column;
      min-width: 0;
      border-right: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.78);
    }
    .rooms-head {
      padding: 16px;
      border-bottom: 1px solid var(--line);
      display: grid;
      gap: 12px;
    }
    .brand {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
    }
    .brand h1 {
      margin: 0;
      font-size: 22px;
      line-height: 1.05;
    }
    .brand p {
      margin: 3px 0 0;
      color: var(--muted);
      font-size: 12px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 2px 8px;
      background: var(--panel-soft);
      color: var(--muted);
      font-size: 11px;
      font-weight: 750;
      white-space: nowrap;
    }
    .pill.unread {
      border-color: rgba(154, 90, 0, 0.28);
      background: var(--warn-bg);
      color: var(--warn);
    }
    .pill.error {
      border-color: rgba(178, 59, 59, 0.28);
      background: #fff0f0;
      color: var(--error);
    }
    .field {
      display: grid;
      gap: 5px;
    }
    .field label {
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
    }
    .row {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .btn {
      padding: 7px 11px;
      font-size: 13px;
    }
    .btn.primary {
      border-color: var(--accent);
      background: var(--accent);
      color: var(--accent-ink);
    }
    .btn.ghost {
      background: transparent;
    }
    details {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
    }
    summary {
      cursor: pointer;
      padding: 9px 11px;
      font-size: 13px;
      font-weight: 750;
    }
    .new-room {
      padding: 0 11px 11px;
      display: grid;
      gap: 8px;
    }
    .presence-panel {
      border-bottom: 1px solid var(--line);
      padding: 10px 8px;
      display: grid;
      gap: 8px;
    }
    .presence-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      padding: 0 8px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 800;
    }
    .presence-list {
      display: grid;
      gap: 6px;
    }
    .presence-agent {
      width: 100%;
      display: grid;
      gap: 4px;
      padding: 9px 10px;
      text-align: left;
      background: var(--panel);
    }
    .presence-agent:disabled {
      cursor: default;
      opacity: 1;
    }
    .presence-main {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      min-width: 0;
      font-weight: 800;
    }
    .presence-name {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .presence-note {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .room-list {
      flex: 1;
      min-height: 0;
      overflow: auto;
      padding: 8px;
      display: grid;
      align-content: start;
      gap: 6px;
    }
    .room {
      width: 100%;
      display: grid;
      gap: 5px;
      padding: 10px;
      text-align: left;
      background: transparent;
    }
    .room.active {
      border-color: var(--accent);
      background: var(--panel);
      box-shadow: 0 0 0 3px rgba(11, 111, 107, 0.12);
    }
    .room-title {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      min-width: 0;
      font-weight: 800;
    }
    .room-title span:first-child {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .muted, .meta {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
    }
    .clip {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .chat {
      min-width: 0;
      height: 100vh;
      display: flex;
      flex-direction: column;
      background: var(--bg);
    }
    .chat-head {
      min-height: 72px;
      padding: 14px 18px;
      border-bottom: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.9);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
    }
    .chat-title {
      min-width: 0;
    }
    .chat-title h2 {
      margin: 0;
      font-size: 19px;
      line-height: 1.15;
      overflow-wrap: anywhere;
    }
    .messages {
      flex: 1;
      min-height: 0;
      overflow: auto;
      padding: 18px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .message-row {
      display: flex;
      align-items: flex-end;
      gap: 8px;
    }
    .message-row.mine { justify-content: flex-end; }
    .message-row.system { justify-content: center; }
    .bubble {
      max-width: min(760px, 74%);
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      padding: 9px 11px;
      box-shadow: var(--shadow);
    }
    .mine .bubble {
      border-color: var(--accent);
      background: var(--accent);
      color: var(--accent-ink);
      box-shadow: none;
    }
    .system .bubble {
      max-width: min(680px, 90%);
      background: var(--warn-bg);
      color: #5c4100;
      box-shadow: none;
    }
    .event .bubble {
      border-color: rgba(93, 75, 156, 0.28);
      background: #f5f2ff;
    }
    .bubble-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 4px;
      font-size: 12px;
      font-weight: 800;
    }
    .mine .bubble-head, .mine .meta {
      color: rgba(255, 255, 255, 0.78);
    }
    .subject {
      margin: 0 0 4px;
      font-weight: 800;
      overflow-wrap: anywhere;
    }
    .body {
      margin: 0;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 12px;
      line-height: 1.55;
    }
    .empty {
      margin: auto;
      color: var(--muted);
      text-align: center;
      border: 1px dashed #b7c0c9;
      border-radius: 8px;
      padding: 24px;
      background: rgba(255, 255, 255, 0.65);
    }
    .composer {
      border-top: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.92);
      padding: 12px;
      display: grid;
      gap: 8px;
    }
    .composer-main {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: end;
      gap: 8px;
    }
    .composer-options {
      display: grid;
      grid-template-columns: minmax(120px, 1fr) minmax(110px, 0.7fr) minmax(130px, 0.9fr) auto;
      align-items: end;
      gap: 8px;
    }
    .check {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      min-height: 36px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 750;
      white-space: nowrap;
    }
    .check input {
      width: 16px;
      height: 16px;
      accent-color: var(--accent);
    }
    .details-panel {
      position: fixed;
      inset: 0 0 0 auto;
      z-index: 20;
      width: min(360px, 92vw);
      border-left: 1px solid var(--line);
      background: var(--panel);
      box-shadow: -18px 0 40px rgba(23, 26, 31, 0.14);
      padding: 16px;
      overflow: auto;
    }
    .details-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 12px;
    }
    .details-head h3 {
      margin: 0;
      font-size: 16px;
    }
    .section {
      border-top: 1px solid var(--line);
      padding-top: 12px;
      margin-top: 12px;
      display: grid;
      gap: 8px;
    }
    .agent {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 9px;
      display: grid;
      gap: 4px;
    }
    .toast {
      position: fixed;
      left: 50%;
      bottom: 18px;
      transform: translateX(-50%);
      z-index: 30;
      max-width: min(560px, calc(100vw - 24px));
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--ink);
      color: white;
      padding: 9px 12px;
      box-shadow: var(--shadow);
      font-size: 13px;
    }
    @media (max-width: 860px) {
      body { overflow: auto; }
      .app {
        display: block;
        height: auto;
        min-height: 100vh;
      }
      .rooms {
        height: auto;
        max-height: 46vh;
        border-right: 0;
        border-bottom: 1px solid var(--line);
      }
      .chat {
        min-height: 54vh;
        height: auto;
      }
      .messages {
        min-height: 360px;
      }
      .composer-options {
        grid-template-columns: 1fr 1fr;
      }
    }
    @media (max-width: 560px) {
      .rooms-head, .chat-head, .messages {
        padding-left: 12px;
        padding-right: 12px;
      }
      .chat-head {
        align-items: flex-start;
        display: grid;
      }
      .bubble {
        max-width: 88%;
      }
      .composer-main {
        grid-template-columns: 1fr;
      }
      .composer-options {
        grid-template-columns: 1fr;
      }
      .composer .btn.primary {
        width: 100%;
      }
    }
  </style>
</head>
<body>
  <div class="app">
    <aside class="rooms">
      <div class="rooms-head">
        <div class="brand">
          <div>
            <h1>raft</h1>
            <p>agent collaboration protocol</p>
          </div>
          <span id="status-pill" class="pill">loading</span>
        </div>
        <div class="field">
          <label for="agent-input">Agent</label>
          <input id="agent-input" autocomplete="off" spellcheck="false">
        </div>
        <div class="field">
          <label for="search-input">Search</label>
          <input id="search-input" autocomplete="off" spellcheck="false">
        </div>
        <div class="row">
          <button id="refresh-button" class="btn primary" type="button">Refresh</button>
          <button id="unread-button" class="btn" type="button">Unread</button>
        </div>
        <details>
          <summary>New chat</summary>
          <div class="new-room">
            <input id="private-to-input" autocomplete="off" spellcheck="false" placeholder="agent ids">
            <input id="private-topic-input" autocomplete="off" spellcheck="false" placeholder="topic">
            <button id="open-private-button" class="btn" type="button">Open</button>
          </div>
        </details>
        <details>
          <summary>New channel</summary>
          <div class="new-room">
            <input id="channel-id-input" autocomplete="off" spellcheck="false" placeholder="channel id">
            <input id="channel-members-input" autocomplete="off" spellcheck="false" placeholder="members">
            <button id="create-channel-button" class="btn" type="button">Create</button>
          </div>
        </details>
      </div>
      <section class="presence-panel" aria-label="Live agents">
        <div class="presence-head">
          <span>Live agents</span>
          <span id="presence-count" class="pill">0</span>
        </div>
        <div id="presence-list" class="presence-list"></div>
      </section>
      <nav id="conversation-list" class="room-list" aria-label="Conversations"></nav>
    </aside>
    <main class="chat">
      <header class="chat-head">
        <div class="chat-title">
          <h2 id="room-title">No chat selected</h2>
          <div id="room-subtitle" class="meta"></div>
        </div>
        <div class="row">
          <button id="join-button" class="btn primary" type="button" hidden>Join</button>
          <button id="details-button" class="btn ghost" type="button">Info</button>
        </div>
      </header>
      <section id="message-list" class="messages" aria-live="polite"></section>
      <form id="composer" class="composer" hidden>
        <div class="composer-main">
          <textarea id="body-input" name="body" placeholder="Message"></textarea>
          <button class="btn primary" type="submit">Send</button>
        </div>
        <div class="composer-options">
          <div class="field">
            <label for="to-input">To</label>
            <input id="to-input" name="to" autocomplete="off" spellcheck="false">
          </div>
          <div class="field">
            <label for="kind-input">Kind</label>
            <select id="kind-input" name="kind">
              <option value="message">message</option>
              <option value="event">event</option>
              <option value="receipt">receipt</option>
            </select>
          </div>
          <div class="field">
            <label for="needs-input">Needs reply</label>
            <select id="needs-input" name="needs_response_from"></select>
          </div>
          <label class="check">
            <input id="ack-input" name="requires_ack" type="checkbox">
            Ack
          </label>
        </div>
      </form>
    </main>
    <aside id="details-panel" class="details-panel" hidden>
      <div class="details-head">
        <h3>Info</h3>
        <button id="details-close" class="btn" type="button">Close</button>
      </div>
      <div id="details-content"></div>
    </aside>
  </div>
  <div id="toast" class="toast" hidden></div>
  <script>
    const state = {
      agent: new URLSearchParams(location.search).get("agent") || "codex",
      selected: null,
      snapshot: null,
      unreadOnly: false,
      query: "",
      detailsOpen: false,
      renderedRoom: null,
      forceScrollBottom: false
    };

    const $ = (id) => document.getElementById(id);
    $("agent-input").value = state.agent;

    function fmtTime(value) {
      if (!value) return "never";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return date.toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
    }

    function roomKind(conversation) {
      if (conversation.channel) return "channel";
      if (conversation.private) return "private";
      return "chat";
    }

    function latestMessage(conversation) {
      return conversation.messages[conversation.messages.length - 1] || null;
    }

    function roomPreview(conversation) {
      const message = latestMessage(conversation);
      if (!message) return "No messages";
      return message.subject || message.body || message.kind;
    }

    async function loadSnapshot({ quiet = false } = {}) {
      if (!quiet) setStatus("loading");
      const agent = $("agent-input").value.trim() || "codex";
      state.agent = agent;
      const response = await fetch(`/api/snapshot?agent=${encodeURIComponent(agent)}&limit=160`, { cache: "no-store" });
      if (!response.ok) {
        setStatus("error", "error");
        throw new Error(await response.text());
      }
      state.snapshot = await response.json();
      if (!state.selected || !state.snapshot.conversations.some((item) => item.id === state.selected)) {
        state.selected = state.snapshot.conversations[0]?.id || null;
      }
      history.replaceState(null, "", `?agent=${encodeURIComponent(agent)}`);
      render();
      setStatus("live");
    }

    function filteredConversations() {
      if (!state.snapshot) return [];
      const query = state.query.toLowerCase();
      return state.snapshot.conversations.filter((conversation) => {
        if (state.unreadOnly && conversation.unread_count === 0) return false;
        if (!query) return true;
        const haystack = [
          conversation.id,
          conversation.participants.join(" "),
          roomPreview(conversation)
        ].join(" ").toLowerCase();
        return haystack.includes(query);
      });
    }

    function selectedConversation() {
      return state.snapshot?.conversations.find((item) => item.id === state.selected) || null;
    }

    function render() {
      if (!state.snapshot) return;
      renderPresence(state.snapshot.agents);
      renderRooms(filteredConversations());
      renderChat(selectedConversation());
      renderDetails();
    }

    function renderPresence(agents) {
      const list = $("presence-list");
      list.textContent = "";
      const liveAgents = agents
        .filter((agent) => agent.active)
        .sort((left, right) => {
          if (left.id === state.agent) return -1;
          if (right.id === state.agent) return 1;
          return activityRank(left.current_state) - activityRank(right.current_state) || left.id.localeCompare(right.id);
        });
      $("presence-count").textContent = liveAgents.length;
      if (liveAgents.length === 0) {
        const empty = document.createElement("div");
        empty.className = "muted";
        empty.textContent = "No live agents";
        list.append(empty);
        return;
      }
      for (const agent of liveAgents) {
        const node = document.createElement("button");
        node.type = "button";
        node.className = "presence-agent";
        node.disabled = agent.id === state.agent;
        node.title = agent.id === state.agent ? "This is you" : `Open chat with ${agent.mention}`;
        node.addEventListener("click", () => openPrivateChat(agent.id, ""));
        const top = document.createElement("div");
        top.className = "presence-main";
        const name = document.createElement("span");
        name.className = "presence-name";
        name.textContent = agent.mention;
        const status = document.createElement("span");
        status.className = `pill${agent.current_state === "blocked" ? " error" : ""}`;
        status.textContent = agent.current_state;
        top.append(name, status);
        const note = document.createElement("div");
        note.className = "presence-note";
        note.textContent = activityText(agent);
        node.append(top, note);
        list.append(node);
      }
    }

    function activityRank(value) {
      return { blocked: 0, working: 1, idle: 2, away: 3 }[value] ?? 4;
    }

    function activityText(agent) {
      if (agent.state_note) return agent.state_note;
      const workspace = agent.workspace ? agent.workspace.split("/").filter(Boolean).pop() : "";
      if (workspace) return `${workspace} | seen ${fmtTime(agent.last_seen_at)}`;
      return `seen ${fmtTime(agent.last_seen_at)}`;
    }

    function asksLabel(conversation) {
      const open = conversation.open_asks || 0;
      if (open === 0) return "no open asks";
      return open === 1 ? "1 open ask" : `${open} open asks`;
    }

    function renderRooms(conversations) {
      const list = $("conversation-list");
      list.textContent = "";
      if (conversations.length === 0) {
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = "No chats";
        list.append(empty);
        return;
      }
      for (const conversation of conversations) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = `room${conversation.id === state.selected ? " active" : ""}`;
        button.addEventListener("click", () => {
          state.selected = conversation.id;
          render();
        });

        const title = document.createElement("div");
        title.className = "room-title";
        const name = document.createElement("span");
        name.textContent = conversation.id;
        title.append(name);
        if (conversation.unread_count > 0) {
          const unread = document.createElement("span");
          unread.className = "pill unread";
          unread.textContent = conversation.unread_count;
          title.append(unread);
        }

        const meta = document.createElement("div");
        meta.className = "muted";
        meta.textContent = `${roomKind(conversation)} | ${asksLabel(conversation)}`;
        const preview = document.createElement("div");
        preview.className = "muted clip";
        preview.textContent = roomPreview(conversation);
        button.append(title, meta, preview);
        list.append(button);
      }
    }

    function renderChat(conversation) {
      const list = $("message-list");
      const previousRoom = state.renderedRoom;
      const nextRoom = conversation ? conversation.id : null;
      const roomChanged = previousRoom !== nextRoom;
      const previousScrollTop = list.scrollTop;
      const previousScrollHeight = list.scrollHeight;
      const shouldStickToBottom = roomChanged || state.forceScrollBottom || isNearBottom(list);
      list.textContent = "";
      const composer = $("composer");
      const join = $("join-button");
      join.hidden = true;
      join.onclick = null;
      composer.hidden = true;
      state.renderedRoom = nextRoom;
      state.forceScrollBottom = false;

      if (!conversation) {
        $("room-title").textContent = "No chat selected";
        $("room-subtitle").textContent = "";
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = "No chats";
        list.append(empty);
        return;
      }

      $("room-title").textContent = conversation.id;
      $("room-subtitle").textContent = `${conversation.participants.join(", ")} | ${roomKind(conversation)} | ${asksLabel(conversation)}`;

      if (conversation.channel && !conversation.joined) {
        join.hidden = false;
        join.onclick = () => joinChannel(conversation.id);
      }

      if (conversation.messages.length === 0) {
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = conversation.joined ? "No messages" : "Join to read";
        list.append(empty);
      } else {
        for (const message of conversation.messages) {
          list.append(renderMessage(message));
        }
      }

      if (conversation.joined) {
        updateComposer(conversation);
        composer.hidden = false;
      }
      requestAnimationFrame(() => {
        if (shouldStickToBottom) {
          list.scrollTop = list.scrollHeight;
        } else {
          const heightDelta = list.scrollHeight - previousScrollHeight;
          list.scrollTop = Math.max(0, previousScrollTop + Math.min(0, heightDelta));
        }
      });
    }

    function isNearBottom(node) {
      return node.scrollHeight - node.scrollTop - node.clientHeight < 72;
    }

    function renderMessage(message) {
      const row = document.createElement("article");
      const mine = message.from === state.agent;
      const system = message.kind === "system";
      row.className = `message-row${mine ? " mine" : ""}${system ? " system" : ""}${message.kind === "event" ? " event" : ""}`;

      const bubble = document.createElement("div");
      bubble.className = "bubble";
      const head = document.createElement("div");
      head.className = "bubble-head";
      const from = document.createElement("span");
      from.textContent = system ? "raft" : message.from;
      const at = document.createElement("span");
      at.textContent = fmtTime(message.created_at);
      head.append(from, at);
      bubble.append(head);

      if (message.subject) {
        const subject = document.createElement("p");
        subject.className = "subject";
        subject.textContent = message.subject;
        bubble.append(subject);
      }

      const body = document.createElement("pre");
      body.className = "body";
      body.textContent = message.body || "(empty)";
      bubble.append(body);

      const metaBits = [];
      if (!system) metaBits.push(`to ${message.to.join(", ")}`);
      if (message.kind !== "message" && !system) metaBits.push(message.kind);
      if (message.requires_ack) metaBits.push("ack");
      if (message.needs_response_from && message.needs_response_from.length > 0) {
        metaBits.push(`needs reply: ${message.needs_response_from.join(", ")}`);
      }
      if (message.unread) metaBits.push("unread");
      if (metaBits.length > 0) {
        const meta = document.createElement("div");
        meta.className = "meta";
        meta.textContent = metaBits.join(" | ");
        bubble.append(meta);
      }
      row.append(bubble);
      return row;
    }

    function updateComposer(conversation) {
      const defaultTo = conversation.channel
        ? "*"
        : conversation.participants.filter((participant) => participant !== state.agent).join(",") || "*";
      if (!$("to-input").value || $("to-input").dataset.room !== conversation.id) {
        $("to-input").value = defaultTo;
        $("to-input").dataset.room = conversation.id;
      }
      const needs = $("needs-input");
      const previous = needs.value;
      needs.textContent = "";
      const blank = document.createElement("option");
      blank.value = "";
      blank.textContent = "No reply needed";
      needs.append(blank);
      for (const participant of conversation.participants) {
        if (participant === state.agent) continue;
        const option = document.createElement("option");
        option.value = participant;
        option.textContent = participant;
        needs.append(option);
      }
      needs.value = [...needs.options].some((option) => option.value === previous) ? previous : "";
    }

    function renderDetails() {
      $("details-panel").hidden = !state.detailsOpen;
      if (!state.detailsOpen || !state.snapshot) return;
      const content = $("details-content");
      content.textContent = "";
      const conversation = selectedConversation();
      content.append(detailBlock("Bus", [
        `root: ${state.snapshot.root}`,
        `updated: ${fmtTime(state.snapshot.generated_at)}`,
        `active agents: ${state.snapshot.totals.active_agents}`,
        `visible chats: ${state.snapshot.totals.conversations}`
      ]));
      if (conversation) {
        content.append(detailBlock("Chat", [
          `id: ${conversation.id}`,
          `kind: ${roomKind(conversation)}`,
          `joined: ${conversation.joined ? "yes" : "no"}`,
          `open asks: ${conversation.open_asks || 0}`,
          `messages: ${conversation.message_count}`
        ]));
      }
      const section = document.createElement("div");
      section.className = "section";
      const title = document.createElement("strong");
      title.textContent = "Agents";
      section.append(title);
      for (const agent of state.snapshot.agents) {
        const node = document.createElement("div");
        node.className = "agent";
        const top = document.createElement("div");
        top.className = "row";
        const name = document.createElement("strong");
        name.textContent = agent.mention;
        const status = document.createElement("span");
        status.className = `pill${agent.active ? "" : " error"}`;
        status.textContent = agent.active ? agent.current_state : "stale";
        top.append(name, status);
        const meta = document.createElement("div");
        meta.className = "meta";
        meta.textContent = `${agent.capabilities.join(", ") || "no capabilities"} | seen ${fmtTime(agent.last_seen_at)}`;
        node.append(top, meta);
        if (agent.id !== state.agent) {
          const chat = document.createElement("button");
          chat.className = "btn";
          chat.type = "button";
          chat.textContent = "Chat";
          chat.addEventListener("click", () => openPrivateChat(agent.id, ""));
          node.append(chat);
        }
        section.append(node);
      }
      content.append(section);
    }

    function detailBlock(titleText, lines) {
      const section = document.createElement("div");
      section.className = "section";
      const title = document.createElement("strong");
      title.textContent = titleText;
      section.append(title);
      for (const line of lines) {
        const node = document.createElement("div");
        node.className = "meta";
        node.textContent = line;
        section.append(node);
      }
      return section;
    }

    function setStatus(value, tone = "") {
      const pill = $("status-pill");
      pill.textContent = value;
      pill.className = `pill${tone === "error" ? " error" : ""}`;
    }

    function toast(message) {
      const node = $("toast");
      node.textContent = message;
      node.hidden = false;
      clearTimeout(toast.timer);
      toast.timer = setTimeout(() => {
        node.hidden = true;
      }, 2200);
    }

    async function apiPost(path, payload) {
      setStatus("sending");
      const response = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const text = await response.text();
      let result = {};
      if (text) {
        try {
          result = JSON.parse(text);
        } catch {
          result = { ok: false, error: text };
        }
      }
      if (!response.ok || result.ok === false) {
        throw new Error(result.error || `request failed with ${response.status}`);
      }
      return result;
    }

    async function openPrivateChat(to, topic) {
      const target = (to || "").trim();
      if (!target) {
        toast("Agent required");
        return;
      }
      try {
        const result = await apiPost("/api/open", {
          agent: state.agent,
          to: target,
          topic: (topic || "").trim()
        });
        state.selected = result.conversation_id;
        state.forceScrollBottom = true;
        $("private-to-input").value = "";
        $("private-topic-input").value = "";
        await loadSnapshot();
        toast("Chat ready");
      } catch (error) {
        setStatus("error", "error");
        toast(error.message);
      }
    }

    async function createChannel() {
      const channel = $("channel-id-input").value.trim();
      if (!channel) {
        toast("Channel required");
        return;
      }
      try {
        const result = await apiPost("/api/channel", {
          agent: state.agent,
          channel,
          members: $("channel-members-input").value.trim()
        });
        state.selected = result.conversation_id;
        state.forceScrollBottom = true;
        $("channel-id-input").value = "";
        $("channel-members-input").value = "";
        await loadSnapshot();
        toast("Channel ready");
      } catch (error) {
        setStatus("error", "error");
        toast(error.message);
      }
    }

    async function joinChannel(channel) {
      try {
        await apiPost("/api/join", { agent: state.agent, channel });
        state.selected = channel;
        state.forceScrollBottom = true;
        await loadSnapshot();
        toast("Joined");
      } catch (error) {
        setStatus("error", "error");
        toast(error.message);
      }
    }

    async function sendCurrentMessage(event) {
      event.preventDefault();
      const conversation = selectedConversation();
      if (!conversation) return;
      const body = $("body-input").value.trim();
      if (!body) {
        toast("Message required");
        return;
      }
      const payload = {
        agent: state.agent,
        conversation: conversation.channel ? null : conversation.id,
        channel: conversation.channel ? conversation.id : null,
        to: $("to-input").value.trim() || "*",
        subject: "",
        body,
        kind: $("kind-input").value || "message",
        requires_ack: $("ack-input").checked,
        needs_response_from: $("needs-input").value ? [$("needs-input").value] : []
      };
      if (payload.kind !== "message") payload.needs_response_from = [];
      try {
        state.forceScrollBottom = true;
        await apiPost("/api/send", payload);
        $("body-input").value = "";
        $("ack-input").checked = false;
        $("needs-input").value = "";
        await loadSnapshot();
        toast("Sent");
      } catch (error) {
        setStatus("error", "error");
        toast(error.message);
      }
    }

    $("refresh-button").addEventListener("click", () => loadSnapshot().catch((error) => toast(error.message)));
    $("open-private-button").addEventListener("click", () => openPrivateChat($("private-to-input").value, $("private-topic-input").value));
    $("create-channel-button").addEventListener("click", () => createChannel());
    $("unread-button").addEventListener("click", () => {
      state.unreadOnly = !state.unreadOnly;
      $("unread-button").classList.toggle("primary", state.unreadOnly);
      render();
    });
    $("agent-input").addEventListener("keydown", (event) => {
      if (event.key === "Enter") loadSnapshot().catch((error) => toast(error.message));
    });
    $("search-input").addEventListener("input", (event) => {
      state.query = event.target.value;
      render();
    });
    $("details-button").addEventListener("click", () => {
      state.detailsOpen = !state.detailsOpen;
      renderDetails();
    });
    $("details-close").addEventListener("click", () => {
      state.detailsOpen = false;
      renderDetails();
    });
    $("composer").addEventListener("submit", sendCurrentMessage);

    loadSnapshot().catch((error) => {
      setStatus("error", "error");
      const list = $("message-list");
      list.textContent = "";
      const empty = document.createElement("div");
      empty.className = "empty";
      empty.textContent = error.message;
      list.append(empty);
    });
    setInterval(() => loadSnapshot({ quiet: true }).catch(() => setStatus("offline", "error")), 5000);
  </script>
</body>
</html>
"##;
