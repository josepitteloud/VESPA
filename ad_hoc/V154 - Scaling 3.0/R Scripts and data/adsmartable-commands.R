#Further breakdown of results so that we can get scores for when we incorporate
#adsmartable boxes
#As we are using the correlation method convert the covariance matrix to a corrletion matrix
cor_matrix = cov2cor(cov_matrix)
cor_matrix
#Following the code from princomp.R
edc = eigen(cor_matrix, symmetric = TRUE)

#Need to scale the data using the info obtained from princomp() code.
scaled_weighted_x = scale(weighted_x[,-8], 
                           center=as.numeric(cov_scaling$center), 
                           scale=as.numeric(sqrt(diag(cov_scaling$cov))))
head(scaled_weighted_x)
#Multiply by eigenvectors to get scores
weighted_scores = scaled_weighted_x %*% edc$vectors

#Set column names
colnames(weighted_scores) = c('Comp.1','Comp.2','Comp.3','Comp.4','Comp.5','Comp.6','Comp.7')

#explicitly set princomp object's scores to be those calculated
pr_scaling$scores = weighted_scores

#Can use biplot as normal, though note that we are not using weights at present
#The xlabs argument is used as it is much quicker when used in this form as
#otherwise it uses row labels.
biplot(pr_scaling, xlabs=as.factor(weighted_x[,1]))
#Look at including the third dimension
biplot(pr_scaling2, xlabs=as.factor(weighted_x2[,1]), choices = c(1,3))
biplot(pr_scaling2, xlabs=as.factor(weighted_x2[,1]), choices = c(2,3))

#Recreating the plots using ggplot, without the arrows.

#Create a sky palette of colours
skyPalette = c(rgb(193,0,31, maxColorValue=255)
               ,rgb(193,0,104, maxColorValue=255)
               ,rgb(234,185,12, maxColorValue=255)
               ,rgb(183,199,42, maxColorValue=255)
               ,rgb(0,156,221, maxColorValue=255)
               ,rgb(0,0,0, maxColorValue=255)
               ,rgb(102,0,0, maxColorValue=255)
               ,rgb(102,0,102, maxColorValue=255)
               ,rgb(204,51,0, maxColorValue=255)
               ,rgb(0,102,51, maxColorValue=255)
               ,rgb(0,0,153, maxColorValue=255)
               ,rgb(9,113,186, maxColorValue=255)
               ,rgb(2,32,66, maxColorValue=255)
               ,rgb(223,0,127, maxColorValue=255)
               ,rgb(102,44,131, maxColorValue=255)
)

library(ggplot2)
p = ggplot(as.data.frame(weighted_scores), aes(weighted_scores[,1], weighted_scores[,2]))
attach(weighted_x)

#Tell R that you want a points plot (the geom_point() command) and that you
#want the colours to be those from the skyPalette, with the colours dependent on
#the value of universe
p + geom_point(aes(colour=as.factor(universe_no))) + scale_colour_manual(values=skyPalette)
p + geom_point(aes(colour=as.factor(isba_tv_region_no))) + scale_colour_manual(values=skyPalette)
p + geom_point(aes(colour=as.factor(hhcomposition_no))) + scale_colour_manual(values=skyPalette)
p + geom_point(aes(colour=as.factor(tenure_no))) + scale_colour_manual(values=skyPalette)
p + geom_point(aes(colour=as.factor(package_no))) + scale_colour_manual(values=skyPalette)
p + geom_point(aes(colour=as.factor(boxtype_no))) + scale_colour_manual(values=skyPalette)
p + geom_point(aes(colour=as.factor(adsmartable_no))) + scale_colour_manual(values=skyPalette)

#From the plots and loadings we need to look at the fourth component to get a better idea of the
#relationship so we can use either the scatterplot3d or rgl libraries
library(scatterplot3d)
scatterplot3d(weighted_scores[,c(1,2,4)], color=skyPalette[isba_tv_region_no], pch=19)
#The angle command allows some control over the placement of axes
scatterplot3d(weighted_scores[,c(1,2,4)], color=skyPalette[isba_tv_region_no], pch=19, angle=72)

#Couldn't really find a way to be more precise than using the angle command so can also use
#the rgl command plot3d
library(rgl)
plot3d(weighted_scores[,c(1,2,4)], col=skyPalette[isba_tv_region_no], aspect=TRUE, size=3)

#Interesting to look at plot from the adsmartable 'perspective'
plot3d(weighted_scores, col = skyPalette[adsmartable_no+1])

#To save current snapshot
#rgl.snapshot('biplot_123_region_noadsmart_1.png')

#Histogram showing range of values for itv region in the third component
library(lattice)
histogram(~weighted_scores[,4] | isba_tv_region_no)
