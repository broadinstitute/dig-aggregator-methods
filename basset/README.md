# basset

The Baset method is a PyTorch Python ML script that predicts chromatin accessibility of genomic regions given their 600bp sequence

The Basset model was published by David Kelley converted to pytorch by Roman Kreuzhuber. It categorically predicts probabilities of accesible genomic regions in 164 cell types (ENCODE project and Roadmap Epigenomics Consortium). Data was generated using DNAse-seq. The sequence length the model uses as input is 600bp. The input of the tensor has to be (N, 4, 600, 1) for N batch samples, 600bp window size and 4 nucleotides (one hot encoded ACGT). Per sample, 164 tissue probabilities of accessible chromatin will be predicted.

## Stages

These are the stages of basset.

### BassetStage

The first and only stage of the Basset model predictions are a python script which takes as input a variant json file, then computes the chromatin accessibility predictions for both the reference and alernate sequences using the variant as the mid point of the 600bp region. The result given is the difference between the reference and alternate predictions.
