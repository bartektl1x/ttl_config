# Historical retention material

The prompts in [`agent-prompts/`](agent-prompts/) record earlier investigations
and task definitions. They refer to older paths and assumptions and must not
be used as current implementation instructions.

The duplicate `ttl_config_v2/` prototype was removed from the working tree;
Git history retains its implementation, tests, and sample workbooks. The
packaged baseline lives in `src/ttl_config/`, and the isolated table-lineage
design lives in `inheritance_v2/`.

For the production Part 2 PR, start at
[`inheritance_v2/PRODUCTION_PART_2_REVIEW.md`](../inheritance_v2/PRODUCTION_PART_2_REVIEW.md),
then read [`inheritance_v2/PORT_TO_PRODUCTION.md`](../inheritance_v2/PORT_TO_PRODUCTION.md).
