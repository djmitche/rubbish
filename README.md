# cas - Content Addressible Storage

# TO LEARN

 * why do I need `.to_string()` everywhere?

# TODO

 * use rustc_serialize everywhere, since we use its FromHex/ToHex
 * refactor hashing into hash.rs, so that the vec isn't public
 * instantiate Storage with a single type, so that there's no risk of serialize/deserialize getting different types

