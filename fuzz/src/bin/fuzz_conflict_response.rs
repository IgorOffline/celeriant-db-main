fn main() {
    afl::fuzz!(|data: &[u8]| {
        celeriant_fuzz::run_conflict_response(data);
    });
}
