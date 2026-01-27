# CoSy Luigi Playground
This repository shows how you can use CoSy to generate Luigi pipelines from individual Tasks. 

- `test_luigi_simple.py` shows how to make a simple pipeline, output depends on either `A` or `B`. 
- `test_luigi_non_global_shape.py` shows how to generate pipelines with different tasks, where one of them can infinitely reoccur in sequence. 
- `test_luigi_global_shape.py` shows how to make a slightly more complicated pipeline, and how a multiarrow type can also be used to describe the global structure of a pipeline. 

Note that the `global shape` and `local shape` approaches can usually not be trivially mixed, as the arrow type means different things. 

- `global shape`: `A → B → C` is interpreted as a `pipeline` that first does A, then B, then C. 
- `local shape`: `A → B → C` is interpreted as a `task` that requires A and B, then returns a C. 

To mix and match these, one would need to resolve this conflict, perhaps by utilizing product types `A x B` to denote a `local shape` with multiple dependencies instead. 

# Installation
To play around with this repository, simply run: 

```pip install -r requirements.txt```

# Caution
The requirements do not pin the version of CoSy. CoSy is still rapidly changing, should we decide to do another pre-release, these examples are likely to need updating. If this is the case, write me an e-mail iin case I forget about this repo as a dependency, I will update this repo then. 
