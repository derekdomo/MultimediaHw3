result=dlmread('box.dat');
X=sptensor([result(:,1) result(:,2) result(:,3)],result(:,4));
answer=cp_als(X,2);
a1=answer.U{1}(:,1);
b1=answer.U{2}(:,1);
c1=answer.U{3}(:,1);
disp('count of src-IDs:')
sum(a1>0.001)
disp('src-IDs:')
find(a1>0.001)
disp('count of dst-IDs')
sum(b1>0.001)
disp('dst-IDs:')
find(b1>0.001)
disp('port')
find(c1>0.001)
exit