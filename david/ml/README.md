
Self organizing map example 

som_data.py
 * generate input data that will be used later
som_alpha.py 
 * loads data and builds map 
som.py
 * self-organizing map code

PMOM Example

make_pmom_data.py 
 * generate the pmom input data
pmom.py
 * this has two entry points.  first you'll run "train" and specify the date range that you want to train over.  note that this saves the resulting models to a pickle file that can be recalled later for use going forward.  once we have trained the static model historically, we can run using 'generate_signal_history2' which will read in the model, and then run a given date and output the probability that a security is in the "top half" 

    
