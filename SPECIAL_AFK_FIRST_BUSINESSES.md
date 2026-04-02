# SPECIAL AFK-FIRST BUSINESSES (Premium Late-Game Design)

This document upgrades four high-tier businesses into premium AFK-first systems.

Design rule: players make a few clear choices in ~2 minutes, then the business runs mostly on its own.

---

## 1) Liquor Store

### 1) Name
**Liquor Store**

### 2) Cost
**100,000,000**

### 3) Core fantasy
Exclusive bottles, limited drops, nightlife spikes, and high-status merch runs.

### 4) Exclusive mechanic
**Drop Shelf System**
- At run start, the player sets their stock style (Cheap vs Premium).
- Stock drains passively over the run; low stock weakens earnings.
- Rare bottle drops can appear and create a short “sold out” money spike.

**1–2 line explain version (for UI):**
“Pick your stock style before the night starts. Premium earns bigger, but stock pressure hits harder if you run dry.”

### 5) Exact 2-minute start-of-run actions
1. **Restock** (fills stock meter before opening)
2. Choose **Cheap Stock** or **Premium Stock**
3. **Push Hype** (one-time chance boost for Rush Night)

### 6) What happens after the 2-minute mark
- Store runs automatically in AFK mode.
- Stock drains at a fixed passive pace.
- Rush Night can trigger once per run.
- Rare Bottle drop can randomly trigger and add a payout spike.
- No constant input required.

### 7) Risk/reward design
- **Cheap Stock:** lower buy cost, safer floor, lower peak payout.
- **Premium Stock:** higher buy cost, stronger payout, bigger penalty if stock runs low.
- **Push Hype:** better Rush Night odds but adds volatility (higher high, lower low).

### 8) How it stays AFK-friendly
- All meaningful choices happen up front.
- No mandatory mid-run tapping.
- Outcome reads clearly from stock style + one or two run events.

### 9) End-of-run summary design
**Header:** `🍾 Night Recap`

**Top chips:**
- `Stock: Premium` / `Stock: Cheap`
- `Rush Night: Hit` / `Missed`
- `Rare Drop: Vintage Crown` / `No Drop`

**Impact list:**
- **Helped:** “Rush Night popped off (+X%)”
- **Helped:** “Rare bottle sold out in minutes (+$X)”
- **Hurt:** “Stock ran low in late hours (-X%)”

**Payout block:**
- Base payout
- Event bonus
- Stock pressure penalty
- **Total payout**

### 10) 5 short flavor lines
- “VIP line out the door. Wallets open.”
- “Premium shelf vanished before midnight.”
- “Someone bought six bottles ‘for research.’”
- “Rush Night hit and the register caught fire (financially).”
- “Cheap stock survived. Barely.”

### 11) UI suggestions for the business card/embed
- **Card meter:** `Stock` (high/medium/low color bands)
- **One premium badge:** `Rush Night Ready` if hype pushed
- **Event pulse row:** shows `Rare Drop` when triggered
- Keep the card clean: one meter, max two chips, one payout preview

### 12) Exact button labels using simple words only
- `Restock`
- `Cheap Stock`
- `Premium Stock`
- `Push Hype`
- `Start Run`

### 13) Why this feels premium
It has limited drops, nightlife spikes, and high-status inventory picks that instantly feel above normal “start and wait” businesses.

### 14) Why this is still simple
Three setup taps, one clear stock meter, and automatic passive resolution.

---

## 2) Underground Market

### 1) Name
**Underground Market**

### 2) Cost
**250,000,000**

### 3) Core fantasy
Under-the-table flips, hot items, nerve-based profits, and volatile runs.

### 4) Exclusive mechanic
**Heat Deal System**
- At start, choose **Safe** or **Risky**.
- Lock one deal focus that drives the run.
- A run can discover a **Hot Item** that becomes the main payout engine.

**1–2 line explain version (for UI):**
“Safe gives steady money. Risky can go huge or go cold. Lock one deal and let it ride.”

### 5) Exact 2-minute start-of-run actions
1. Choose **Play Safe** or **Take Risk**
2. **Lock Deal** (pick one featured deal)
3. **Push Hot Deal** (increase Hot Item chance)

### 6) What happens after the 2-minute mark
- Market runs passively.
- Deal payouts tick automatically.
- If Hot Item appears, it boosts run earnings for a period.
- Risky mode has wider payout swings; Safe stays steadier.

### 7) Risk/reward design
- **Play Safe:** tighter payout range, fewer bad runs.
- **Take Risk:** bigger upside, occasional weak runs.
- **Push Hot Deal:** higher chance of a breakout run, but can whiff.

### 8) How it stays AFK-friendly
- Single upfront mode choice drives the whole run.
- No mid-run management needed.
- Results are explained by mode + Hot Item outcome.

### 9) End-of-run summary design
**Header:** `🕶️ Market Recap`

**Top chips:**
- `Mode: Safe` / `Mode: Risky`
- `Locked Deal: [name]`
- `Hot Item: Found` / `No Find`

**Impact list:**
- **Helped:** “Hot Deal carried the run (+$X)”
- **Helped:** “Risk line hit high margin windows (+X%)”
- **Hurt:** “Cold cycle reduced demand (-X%)”

**Payout block:**
- Base deal profit
- Hot Item bonus
- Risk variance adjustment
- **Total payout**

### 10) 5 short flavor lines
- “Safe play, clean money, zero panic.”
- “Risk line printed. You look suspiciously calm.”
- “Hot Item found. Everyone suddenly knows a guy.”
- “Deal went cold, but not broke.”
- “One lock-in. Big swing. No regrets (publicly).”

### 11) UI suggestions for the business card/embed
- **Mode pill:** `Safe` or `Risky`
- **Deal chip:** current locked deal shown as a single line
- **Hot meter:** low/ready/hot state (not a complex graph)
- **Variance preview:** tiny label like `Steady` or `Swingy`

### 12) Exact button labels using simple words only
- `Play Safe`
- `Take Risk`
- `Lock Deal`
- `Push Hot Deal`
- `Start Run`

### 13) Why this feels premium
It delivers elite “big hit” moments with Hot Items and tension that normal passive businesses do not have.

### 14) Why this is still simple
One mode pick, one locked deal, one push button, then AFK.

---

## 3) The Cartel

### 1) Name
**The Cartel**

### 2) Cost
**500,000,000**

### 3) Core fantasy
You run a hidden money machine powered by pressure and control.

### 4) Exclusive mechanic
**Control Grid System**
- Early actions raise or protect a clear **Control meter**.
- Higher Control gives stronger passive earnings.
- Finishing runs with high Control builds a cross-run **Pressure Streak** bonus.

**1–2 line explain version (for UI):**
“Build Control early, then let the machine run. Hold high Control across runs to stack Pressure bonuses.”

### 5) Exact 2-minute start-of-run actions
1. **Expand** (higher upside, can shake control)
2. **Lock Down** (stabilizes control)
3. **Collect Pressure** (locks in streak gain if control is high)

### 6) What happens after the 2-minute mark
- Operation runs passively.
- Control slowly drifts based on starting setup.
- If Control stays high, Pressure Streak grows.
- If Control drops too low, payout loses strength.

### 7) Risk/reward design
- **Expand:** stronger potential earnings, higher control drift risk.
- **Lock Down:** safer control, lower spike potential.
- **Collect Pressure:** rewards smart setup with long-term streak growth.

### 8) How it stays AFK-friendly
- No repeated tapping to maintain Control.
- Meter trend is determined mostly by start choices.
- Run resolves with clean high/medium/low control outcomes.

### 9) End-of-run summary design
**Header:** `🦂 Control Recap`

**Top chips:**
- `Control End: High / Mid / Low`
- `Pressure Streak: +N`
- `Setup: Expand + Lock Down` (or chosen combo)

**Impact list:**
- **Helped:** “Pressure stayed high (+X%)”
- **Helped:** “Streak bonus stacked (+$X)”
- **Hurt:** “Control slipped late (-X%)”

**Payout block:**
- Base network flow
- Control bonus
- Pressure streak bonus
- Control loss penalty
- **Total payout**

### 10) 5 short flavor lines
- “Pressure stayed high. Everyone paid on time.”
- “You expanded hard and it actually worked.”
- “Control slipped for a minute. Message received.”
- “Streak climbed. The machine is learning your name.”
- “Lock Down pressed. Noise disappeared.”

### 11) UI suggestions for the business card/embed
- **Large central meter:** `Control`
- **Streak badge:** `Pressure xN`
- **State stamp:** `Dominant`, `Stable`, or `Slipping`
- Dark premium palette with one accent color for Control level

### 12) Exact button labels using simple words only
- `Expand`
- `Lock Down`
- `Collect Pressure`
- `Start Run`

### 13) Why this feels premium
It introduces cross-run dominance through Control + Pressure Streak, making each run feel like part of a bigger power climb.

### 14) Why this is still simple
One visible meter, three setup actions, and passive auto-resolution.

---

## 4) The Shadow Government

### 1) Name
**The Shadow Government**

### 2) Cost
**1,000,000,000**

### 3) Core fantasy
You pull hidden strings and bend the entire economy from behind the curtain.

### 4) Exclusive mechanic
**Power Doctrine System**
- In the first 2 minutes, lock one doctrine:
  - **Cash Out** (huge personal profit this run)
  - **Call Favors** (boost all businesses this run)
  - **Build Power** (lower now, stronger future power bank)
- A shared **Power meter** fuels doctrine strength.

**1–2 line explain version (for UI):**
“Pick what this run is for: immediate money, network boost, or future power. Build Power early, then let influence do the work.”

### 5) Exact 2-minute start-of-run actions
1. `Build Power`
2. `Call Favors`
3. `Cash Out`
4. `Start Run`

(First press sets doctrine focus; remaining presses become small boosts tied to that focus.)

### 6) What happens after the 2-minute mark
- Shadow system runs passively.
- Doctrine applies automatic effects through the run.
- Global boost (if chosen) applies to all owned active businesses.
- Power meter updates at end with clear gain/spend outcome.

### 7) Risk/reward design
- **Cash Out:** biggest immediate self payout, weakest future growth.
- **Call Favors:** medium self payout, strongest network-wide boost now.
- **Build Power:** smallest immediate payout, strongest next-run power setup.

### 8) How it stays AFK-friendly
- One doctrine decision defines everything.
- Global effect is automatic; no per-business babysitting.
- End screen explains exactly what doctrine did.

### 9) End-of-run summary design
**Header:** `🕳️ Influence Recap`

**Top chips:**
- `Doctrine: Cash Out / Call Favors / Build Power`
- `Power: Start X → End Y`
- `Network Boost: +X%` (if any)

**Impact list:**
- **Helped:** “Favors paid off across your network (+$X)”
- **Helped:** “Power reserve amplified this run (+X%)”
- **Hurt:** “Power spend left less future pressure (-future boost)”

**Payout block:**
- Personal run payout
- Network bonus payout (all businesses)
- Future power bank gain
- **Total value generated**

### 10) 5 short flavor lines
- “One call, six businesses got louder.”
- “Cash Out hit. Nobody asked questions.”
- “Power bank grew. Tomorrow looks unfair.”
- “Favors collected. Debts updated.”
- “You didn’t run the market. You moved it.”

### 11) UI suggestions for the business card/embed
- **Doctrine selector pills** (3 only)
- **Power meter** with simple fill bar
- **Network glow badge:** `All Business Boost Active`
- End summary includes mini table: each business bonus gained this run

### 12) Exact button labels using simple words only
- `Build Power`
- `Call Favors`
- `Cash Out`
- `Start Run`

### 13) Why this feels premium
It is the only business that can directly affect the entire business network, making it feel elite and “final boss.”

### 14) Why this is still simple
Three doctrine buttons, one meter, and automatic network effects.

---

## Shared premium UX rules (for all four)

- **Two-line mechanic explainer max** at the top of each panel.
- **2-minute Setup Timer** visible during start phase; locks choices after timer.
- **One primary meter only** per business (Stock, Hot, Control, Power).
- **Run Story chips** in summary: `Your Choice`, `Big Moment`, `What Helped`, `What Hurt`, `Total Payout`.
- **No wall text** inside embeds; details stay in short bullets.
- **Punchy run result tone** (e.g., “Hot Deal carried the run”).
