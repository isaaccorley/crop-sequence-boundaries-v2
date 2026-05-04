# Paper: CSB v2 for Computers and Electronics in Agriculture

LaTeX source for the *Computers and Electronics in Agriculture* original
research manuscript documenting the open-source Crop Sequence Boundaries
pipeline.

## Contents

```text
paper/
  csb_cea.tex          # Elsevier manuscript source
  csb_cea.pdf          # rendered manuscript
  csb.bib              # BibTeX entries
  Makefile             # latexmk-based build
  README.md            # this file
  elsarticle.cls       # vendored Elsevier document class
  elsarticle-num.bst   # vendored numeric bibliography style
  figures/             # paper figures + matplotlib generators
  sty/                 # vendored LaTeX helpers
```

`elsarticle.cls` / `elsarticle-num.bst` come from the Elsevier author kit.

## Build

```sh
make install # sync repo dependencies from the root Makefile
make build   # build csb_cea.pdf via latexmk
make clean   # drop LaTeX intermediates
```

Requires `latexmk` and a TeX Live distribution with `microtype`,
`hyperref`, `cleveref`, `booktabs`, `siunitx`, `natbib`, `algorithm`,
and `algorithmic`. The Makefile sets `TEXINPUTS` and `BSTINPUTS` so
vendored style files and the Elsevier bibliography style are picked up
locally.

## Submission

Submit `csb_cea.tex`, `csb_cea.pdf`, `csb.bib`, the files under
`figures/`, and the vendored Elsevier style files if Editorial Manager does
not compile from PDF only. The manuscript is intended as an original
research paper, not an application note.

The manuscript is built in `preprint` mode by default (single column,
12 pt, generous margins). To estimate the published two-column production
length, edit the document class options in `csb_cea.tex`:

| Mode          | Document class options       |
| ------------- | ---------------------------- |
| Review        | `[preprint,12pt]`            |
| Two-column 3p | `[final,3p,times,twocolumn]` |
| Two-column 5p | `[final,5p,times,twocolumn]` |

## Figures

Generators live under `figures/make_*.py`. Run them from the repo root
to rebuild the corresponding PDFs:

```sh
uv run python paper/figures/make_acres_scatter.py
uv run python paper/figures/make_per_tile_iou.py
uv run python paper/figures/make_per_class.py
uv run python paper/figures/make_stage_breakdown.py
uv run python paper/figures/make_bottom3_montage.py
```

`hero_conus.pdf` and `pipeline.pdf` are produced separately.
