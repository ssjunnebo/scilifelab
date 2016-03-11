library(pheatmap)
args = commandArgs(TRUE)
snames = args[c(1:length(args))]

# FPKM table for genes

# Find no of rows
name <- snames[1]
fname <- paste("tophat_out_", name, "/cufflinks_out_", name, "/genes.fpkm_tracking", sep="")
temp <- read.table(fname, header=T)

e <-matrix(nrow=nrow(temp), ncol= (length(snames)))
colnames(e) <- rep("NO_NAME_ASSIGNED", length(snames))

idx <- 0 

for (name in snames) {
    idx <- idx+1
    fname <- paste("tophat_out_", name, "/cufflinks_out_", name, "/genes.fpkm_tracking", sep="")
    temp <- read.table(fname, header=T)
    colnames(e)[idx] <- name
    o <- order(temp$tracking_id)
    e[,idx] <- temp$FPKM[o]
    temp.o <- temp[o,]
    print(temp.o[1, ])
} 

# write a more user friendly version file 
d.0 <- data.frame(e)
d.1 <- cbind(Gene_ID=temp[o,5], d.0)
d.2 <- cbind(ENSEMBL_ID=temp[o,1],d.1)

########## clean up cufflinks fpkm table ##########
end=ncol(d.2)


# duplicate gene_ids
dup_gene=unique(d.2[duplicated(d.2[,1]),][,1])

# 1) rm duplicate genes and keep the genes with highest rowsums
# 2) get uniq genes
max_dup=NULL
uniq <- d.2
for (g in dup_gene) {
    dup <- subset(d.2, ENSEMBL_ID==g)
    dup[,'suma']=rowSums(dup[,3:end])  #add rowsums
    dup=dup[with(dup,order(suma)),]    #sort on rowsums
    max_suma <- as.data.frame(strsplit(apply(dup, 2, function(x) max(x, na.rm=TRUE)), "\t"))
    max_dup = rbind(max_dup, max_suma)
    uniq <-uniq[!(uniq$ENSEMBL_ID==g),]
}

# concat uniq and duplicates
all_uniq <- rbind(uniq,max_dup[,1:end])

#remove genes with 0 coverage
keep = rowSums(sapply(all_uniq[, 3:end], as.numeric))>0
all_uniq_0cov_rem = all_uniq[keep, ]

# write fpkm_table
write.table(all_uniq, file="fpkm_table.txt", quote=F, row.names=F, sep="\t")

# make heatmap
all_uniq = sapply(all_uniq[, 3:end], as.numeric)
all_uniq_0cov_rem = sapply(all_uniq_0cov_rem[,3:end], as.numeric)
pdf("FPKM_heatmap.pdf", onefile=FALSE)
if (end>20){pheatmap(cor(all_uniq), symm=T,fontsize = 4)}else{pheatmap(cor(all_uniq), symm=T)}
dev.off()

###-------------- FPKM table for isoforms
# Find no of rows
name <- snames[1]
fname <- paste("tophat_out_", name, "/cufflinks_out_", name, "/isoforms.fpkm_tracking", sep="")
temp <- read.table(fname, header=T)

e <-matrix(nrow=nrow(temp), ncol= (length(snames)))
colnames(e) <- rep("NO_NAME_ASSIGNED", length(snames))

idx <- 0 

for (name in snames) {
    idx <- idx+1
    fname <- paste("tophat_out_", name, "/cufflinks_out_", name, "/isoforms.fpkm_tracking", sep="")
    temp <- read.table(fname, header=T)
    colnames(e)[idx] <- name
    o <- order(temp$tracking_id)
    e[,idx] <- temp$FPKM[o]
    temp.o <- temp[o,]
    print(temp.o[1, ])
} 

# write a more user friendly version file 
d.0 <- data.frame(e)
d.1 <- cbind(Gene_ID=temp[o,4], d.0)
d.2 <- cbind(Transcript_ID=temp[o,1],d.1)

write.table(d.2, file="isoform_fpkm_table.txt", quote=F, row.names=F, sep="\t")


# HTSeq counts

name <- snames[1]
fname <- paste("tophat_out_", name, "/", name, ".counts", sep="")
nlines <- length(count.fields(fname))
temp <- read.table(fname, header=F, nrow = (nlines-5))
h <- matrix(nrow=nrow(temp), ncol=length(snames))
colnames(h) <- rep("NO_NAME_ASSIGNED", length(snames))
rownames(h) <- temp[,1]
idx <- 0

for (name in snames){
    idx <- idx + 1
    fname <- paste("tophat_out_", name, "/", name, ".counts", sep="")
    temp <- read.table(fname, header=F, nrow = (nlines-5))
    colnames(h)[idx]<-name
    h[,idx]<-temp[,2]

}

write.table(h, file="count_table.txt", quote=F, row.names=T, sep="\t")
