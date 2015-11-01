# Projet de Sacha Benamron et Florian Barbaro
#Sacha Benamron
#Florian Barbaro
#Is it useful to use the reducer class as a combiner ? Justify
#The combiner is also known as semi reducer. The features of a reducer and a combiner are the same.
# However a combiner can be different of a reducer, even if the combiner will always implement the reducer interface.
# Combiners can not be used in specific cases which will depend work. The combiner function as a reducer,
# but only on the subset of output Key / Value Mapper. Furthermore The combiner will have a constraint which is that key and value type must
# match the type of the output Mapper.
#So The reducer us allows to be more flexible and we think it may be more advantageous to use.



