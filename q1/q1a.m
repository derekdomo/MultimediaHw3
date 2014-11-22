load('data/mystery_large.txt')

[U,S,V]=svd(mystery_large,0);
S
sort(diag(S),2)
exit
