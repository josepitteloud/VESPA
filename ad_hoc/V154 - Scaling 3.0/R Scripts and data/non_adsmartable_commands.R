#The following is code used to create the covariance matrix, and other statistics, required
#for principal components analysis of the Sky Base

#Load the data used
#Covariance values
load('xtx.RData')
#Values of mean and n
load('X_mean_n.RData')
#Original data used; all variables recalculated as integers with a final column giving the
#'weights' of each set of columns, which is the number of times this set of columns was repeated
load('weighted_x.RData')

#Variable names used
vars = c('universe_no','isba_tv_region_no','hhcomposition_no','tenure_no','package_no','boxtype_no','adsmartable_no')

#Put covariance values into a matrix
cov_matrix=matrix(0, nrow = max(xtx[,1]), ncol = max(xtx[,2]))
for (i in 1:nrow(xtx)) 
{
    cov_matrix[xtx[i,1], xtx[i,2]] = xtx[i,3]
    if (xtx[i,1] != xtx[i,2]) cov_matrix[xtx[i,2], xtx[i,1]] = xtx[i,3]
}
colnames(cov_matrix) = vars
rownames(cov_matrix) = vars

#Create a cov.wt matrix, in reality a list, so that we can use the princomp() command
cov_scaling = list()
cov_scaling$cov = cov_matrix
cov_scaling$center = x_mean_n[1:7]
names(cov_scaling$center) = vars
cov_scaling$n.obs = as.integer(x_mean_n[length(x_mean_n)])
cov_scaling

#Using created list run principal components using the correlation method
pr_scaling = princomp(covmat=cov_scaling, cor=TRUE)
pr_scaling
pr_scaling$loadings

#As we are not using adsmart at the moment remove these rows and columns.
#Note that this will not affect the means as they were calculated on the entire 9.4m base
cov_matrix2 = cov_matrix[-7,-7]

#Create a cov.wt matrix, in reality a list, so that we can use the princomp() command
cov_scaling2 = list()
cov_scaling2$cov = cov_matrix2

#Extra line as we are removing the adsmart variable
weighted_x2 = unique(weighted_x[,1:6])
cov_scaling2$center = x_mean_n[1:6]
names(cov_scaling2$center) = vars[-7]
cov_scaling2$n.obs = as.integer(x_mean_n[length(x_mean_n)])
cov_scaling2

#Using created list run principal components using the correlation method
pr_scaling2 = princomp(covmat=cov_scaling2, cor=TRUE)
pr_scaling2
pr_scaling2$loadings

#Further breakdown of results so that we can get scores
#As we are using the correlation method convert the covariance matrix to a corrletion matrix
cor_matrix2 = cov2cor(cov_matrix2)

#Following the code from princomp.R
edc2 = eigen(cor_matrix2, symmetric = TRUE)

#Need to scale the data using the info obtained from princomp() code.
scaled_weighted_x2 = scale(weighted_x2, 
                          center=as.numeric(cov_scaling2$center), 
                          scale=as.numeric(sqrt(diag(cov_scaling2$cov))))

#Multiply by eigenvectors to get scores
weighted_scores2 = scaled_weighted_x2 %*% edc2$vectors

#Set column names
colnames(weighted_scores2) = c('Comp.1','Comp.2','Comp.3','Comp.4','Comp.5','Comp.6')

#explicitly set princomp object's scores to be those calculated
pr_scaling2$scores = weighted_scores2

#Can use biplot as normal, though note that we are not using weights at present
#The xlabs argument is used as it is much quicker when used in this form as
#otherwise it uses row labels.
biplot(pr_scaling2, xlabs=as.factor(weighted_x2[,1]))
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
p = ggplot(as.data.frame(weighted_scores2), aes(weighted_scores2[,1], weighted_scores2[,2]))
attach(weighted_x2)

#Tell R that you want a points plot (the geom_point() command) and that you
#want the colours to be those from the skyPalette, with the colours dependent on
#the value of universe
p + geom_point(aes(colour=as.factor(universe_no))) + scale_colour_manual(values=skyPalette)
p + geom_point(aes(colour=as.factor(isba_tv_region_no))) + scale_colour_manual(values=skyPalette)
p + geom_point(aes(colour=as.factor(hhcomposition_no))) + scale_colour_manual(values=skyPalette)
p + geom_point(aes(colour=as.factor(tenure_no))) + scale_colour_manual(values=skyPalette)
p + geom_point(aes(colour=as.factor(package_no))) + scale_colour_manual(values=skyPalette)
p + geom_point(aes(colour=as.factor(boxtype_no))) + scale_colour_manual(values=skyPalette)

#From the plots and loadings we need to look at the third component to get a better idea of the
#relationship so we can use either the scatterplot3d or rgl libraries
library(scatterplot3d)
scatterplot3d(weighted_scores2[,1:3], color=skyPalette[isba_tv_region_no], pch=19)
#The angle command allows some control over the placement of axes
scatterplot3d(weighted_scores2[,1:3], color=skyPalette[isba_tv_region_no], pch=19, angle=72)

#Couldn't really find a way to be more precise than using the angle command so can also use
#the rgl command plot3d
library(rgl)
plot3d(weighted_scores2[,1:3], col=skyPalette[isba_tv_region_no], aspect=TRUE, size=3)

#To save current snapshot
rgl.snapshot('biplot_123_region_noadsmart_1.png')

