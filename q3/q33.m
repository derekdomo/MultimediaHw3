result=dlmread('box.dat');
X=sptensor([result(:,1) result(:,2) result(:,3)],result(:,4));
answer=cp_als(X,2);
fig=plot(answer.U{1}(:,1),'*');
saveas(fig,'a1.png');
fig=plot(answer.U{1}(:,2),'*');
saveas(fig,'a2.png');
fig=plot(answer.U{2}(:,1),'*');
saveas(fig,'b1.png');
fig=plot(answer.U{2}(:,2),'*');
saveas(fig,'b2.png');
fig=plot(answer.U{3}(:,1),'*');
saveas(fig,'c1.png');
fig=plot(answer.U{3}(:,1),'*');
saveas(fig,'c2.png');
exit
