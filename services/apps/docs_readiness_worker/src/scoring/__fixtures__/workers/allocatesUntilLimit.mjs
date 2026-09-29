const hoard = []
for (;;) {
  hoard.push(Array.from({ length: 1_000_000 }, () => Math.random()))
}
