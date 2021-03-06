#The following is code used to create the covariance matrix, and other statistics, required
#for principal components analysis of the Sky Base excluding adsmart
setwd('SkyIQ Work folders/Scaling Segment Identification/')

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

#As we are not using universe at the moment remove these rows and columns.
#Note that this will not affect the means as they were calculated on the entire 9.4m base
cov_matrix3 = cov_matrix[-1,-1]

#Create a cov.wt matrix, in reality a list, so that we can use the princomp() command
cov_scaling3 = list()
cov_scaling3$cov = cov_matrix3

#Extra line as we are removing the adsmart variable
weighted_x_nouniverse = unique(weighted_x[,c(2:7,9:14)])
weighted_x3 = unique(weighted_x_nouniverse[,7:12])
cov_scaling3$center = x_mean_n[2:7]
names(cov_scaling3$center) = vars[-1]
cov_scaling3$n.obs = as.integer(x_mean_n[length(x_mean_n)])
cov_scaling3

#Using created list run principal components using the correlation method
pr_scaling3 = princomp(covmat=cov_scaling3, cor=TRUE)
pr_scaling3
pr_scaling3$loadings

#Further breakdown of results so that we can get scores
#As we are using the correlation method convert the covariance matrix to a corrletion matrix
cor_matrix3 = cov2cor(cov_matrix3)

#Following the code from princomp.R
edc3 = eigen(cor_matrix3, symmetric = TRUE)

#Need to scale the data using the info obtained from princomp() code.
scaled_weighted_x3 = scale(weighted_x3, 
                           center=as.numeric(cov_scaling3$center), 
                           scale=as.numeric(sqrt(diag(cov_scaling3$cov))))

#Multiply by eigenvectors to get scores
weighted_scores3 = scaled_weighted_x3 %*% edc3$vectors

#Set column names
colnames(weighted_scores3) = c('Comp.1','Comp.2','Comp.3','Comp.4','Comp.5','Comp.6')

#explicitly set princomp object's scores to be those calculated
pr_scaling3$scores = weighted_scores3

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

#Set x- & y-axes coord_fixed() as we need the plots to be of equal scale
library(ggplot2)
p = NULL
p = ggplot(as.data.frame(weighted_scores3), aes(weighted_scores3[,1], weighted_scores3[,2]))
p = p + xlab("Comp. 1") + ylab("Comp. 2") + coord_fixed()
p = p + theme(legend.title = element_text(size=18, face="bold"))
p = p + theme(legend.text  = element_text(size=15, face="plain"))
attach(weighted_x_nouniverse)

#Tell R that you want a points plot (the geom_point() command) and that you
#want the colours to be those from the skyPalette, with the colours dependent on
#the value of the scaling segment.
p + geom_point(aes(colour=isba_tv_region)) + scale_colour_manual('TV Region', values=skyPalette)
ggsave('scores2d_nouni_by_region.png')
p + geom_point(aes(colour=hhcomposition)) + 
  scale_colour_manual('Household\nComposition', values=skyPalette, labels = c(
    '00: Families',
    '01: Extended family',
    '02: Extended household',
    '03: Pseudo family',
    '04: Single male',
    '05: Single female',
    '06: Male homesharers',
    '07: Female homesharers',
    '08: Mixed homesharers',
    '09: Abbreviated male families',
    '10: Abbreviated female families',
    '11: Multi-occupancy dwelling',
    'U: Unclassified HHComp'
  ))
ggsave('scores2d_nouni_by_hhcomposition.png')
p + geom_point(aes(colour=tenure)) + scale_colour_manual('Tenure', values=skyPalette)
ggsave('scores2d_nouni_by_tenure.png')
p + geom_point(aes(colour=package)) + scale_colour_manual('Package', values=skyPalette)
ggsave('scores2d_nouni_by_package.png')
p + geom_point(aes(colour=boxtype)) + scale_colour_manual('Boxtype', values=skyPalette)
ggsave('scores2d_nouni_by_boxtype.png')
p + geom_point(aes(colour=as.factor(adsmartable))) + 
    scale_colour_manual('AdSmart', values=skyPalette, labels = c(
        '0: Not AdSmartable',
        '1: AdSmartable'
    )) 
ggsave('scores2d_nouni_by_adsmart.png')

#From the plots and loadings we need to look at the third component to get a better idea of the
#relationship so we can use either the scatterplot3d or rgl libraries
p = ggplot(as.data.frame(weighted_scores3), aes(weighted_scores3[,1], weighted_scores3[,3]))
p = p + xlab("Comp. 1") + ylab("Comp. 3") + coord_fixed()
p + geom_point(aes(colour=isba_tv_region)) + scale_colour_manual('TV Region', values=skyPalette)
ggsave('scores2d_nouni_by_region_dim13.png')

library(rgl)
plot3d(weighted_scores3[,1:3], col=skyPalette[isba_tv_region], aspect=TRUE, size=3)
#Take a snapshot at two different angles. Show how flat the regions are
rgl.snapshot('scores3d_nouni_by_region_view1.png')
rgl.snapshot('scores3d_nouni_by_region_view2.png')

