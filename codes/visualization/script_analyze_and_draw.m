addpath(genpath('.'))
clear; close all;
%% filename and draw figure
filename = 'complain_collision'
fileList = readline(sprintf('%s.txt',filename),[1 -1]');

complain_crime_Label = {'agency',...
    'complain type',...
    'location type',...
    'street (complain)','address type','city',...
    'facility type','status',...
    'borough (complain)','channel type',...
    'zipcode (complain)','time (complain)',...
    'offense code','offense code2','success code','offense level',...
    'juridiction','borough (crime)','precinct','location desc',...
    'premise','zipcode (crime)','yymmdd','day','month'...
    'year','time'};

collision_weather_Label = {'borough','street name',...
    'injured person #','killed person #','insured pedestrian #',...
    'killed pedestrian #','injured cyclist #','killed cyclist #','injured motorist #','killed motorist #',...
    'vehicle 1 factor','vehicle 2 factor','vehicle type 1','vehicle type 2',...
    'zipcode','time (collision)','visb','date','day','month','year','time (weather)','spd','temp','prcp'};

complain_weather_Label = {'agency','complain type', 'location type',...
    'street','address type','city','facility type','status',...
    'borough','channel type',...
    'zipcode','time (complain)','visb','yymmdd','day','month','year',...
    'time (weather)','spd','temp','prcp'};


complain_collision_Label = {'agency','complain type', 'location type',...
    'street (complain)','address type','city','facility type','status',...
    'borough (complain)','channel type',...
    'zipcode (complain)','time (complain)','borouth (collision)','street (collision)',...
    'injured person #','killed person #','insured pedestrian #',...
    'killed pedestrian #','injured cyclist #','killed cyclist #',...
    'injured motorist #','killed motorist #',...
    'vehicle 1 factor','vehicle 2 factor','vehicle type 1',...
    'vehicle type 2',...
    'yymmdd','day','month','year','zipcode (collision)','time'};
% Don't know why but 2012, 2013 seems wrong so separately treated

complain_crime_Label = {'agency',...
    'complain type',...
    'location type',...
    'street (complain)','address type','city',...
    'facility type','status',...
    'borough (complain)','channel type',...
    'zipcode (complain)','time (complain)',...
    'offense code','offense code2','success code','offense level',...
    'juridiction','borough (crime)','precinct','location desc',...
    'premise','zipcode (crime)','yymmdd','day','month'...
    'year','time'};

weather_crime_Label = {'visb','time (weather)',...
    'spd','temp','prcp',...
    'offense code','offense code2','success code','offense level',...
    'juridiction','borough (crime)','precinct','location desc',...
    'premise','zipcode (crime)','yymmdd','day','month'...
    'year','time'    };


weather_bike_Label = {'visb','time (weather)',...
    'spd','temp','prcp',...
    'usertype','gender','start zipcode','end zipcode',...
    'trip time', 'yymmdd','day','month','year','time (bike)','age'};

weather_taxi_Label = {'visb','time (weather)',...
    'spd','temp','prcp',...
    'vendor id','passenger count','rate code','payment type',...
    'mta_tax','start zipcode','end zipcode','trip time',...
    'yymmdd','day','month','year','time','tip amount','trip distance',...
    'fare amount','tolls amount','total amount'};

eval(sprintf('Label = %s_Label',filename));

%% Getting matrix and save
yearRange = str2num(cell2mat(cellfun(@(x) x(end-3:end), fileList, 'UniformOutput',false)));


clear corrMat;
clear corrDes;
clear meta;
for i = 1:length(yearRange)
    eval(sprintf('tmp = readline(''%s%d'',[1 -1]'')',filename, yearRange(i)));
    tmp2 = cell2mat(arrayfun(@(x) str2num(cell2mat(x)), tmp(2:2:end), 'UniformOutput', false));
    if size(tmp2,1) ~= size(tmp2,2)
        tmp2(end,:) = []
    end
    tmp2 = tmp2+tmp2';
    if sum(diag(tmp2)) ~= 0
        for j=1:length(tmp2)
            tmp2(j,j) = 0
        end
    end
    
    if length(filename) == length('weather_taxi')
        if filename == 'weather_taxi'
            if yearRange(i) == 2011 || yearRange(i) == 2012
                idxChange = [1:7, 9,10, 12, 15, 16, 17, 18:23, 8, 11, 13, 14];
                tmp2(:,end) = [];
                tmp2(end,:) = [];
                tmp2 = tmp2(idxChange,idxChange);
            end
            if yearRange(i) == 2015
                tmp2(:,10) = [];
                tmp2(10,:) = [];
            end
            if yearRange(i) == 2016
                tmp2(:,[9,11]) = [];
                tmp2([9,11],:) = [];
            end
        end
    end
    
    corrDes(i,:) = dsqrform(tmp2);
    corrMat(:,:,i) = tmp2;
    year(i,1) = yearRange(i);
end

meta.matDes = corrDes;
meta.mat = corrMat;
meta.label = Label;
save(sprintf('%s_all.mat',filename),'-struct','meta');

%% Draw matrix

meta = load(sprintf('%s_all.mat',filename))
drawMat = meta.mat;
drawMatDes = meta.matDes;
maxVal = max(drawMatDes(:));
redMap = cbrewer('seq', 'Reds', 20);

for i = 1:size(drawMat,3)
    f = figure;
    f.Position = [984 318 839 694];
    f.Color = 'w';
    imagesc(drawMat(:,:,i))
    currMax = max(drawMatDes(i,:));
    colormap(redMap(1:end-round((maxVal-currMax)/maxVal*20),:));
    colorbar
    set(gca,'XTick', 1:size(tmp2,1), 'XTickLabel', Label)
    set(gca,'YTick', 1:size(tmp2,1), 'YTickLabel', Label)
    xtickangle(90)
    printname = sprintf('./figure/%s%d',filename,yearRange(i));
    print(f,'-dpng',printname)
    close all;
end



%% Draw temporal plot with three groups
% meta = load(sprintf('%s_all.mat',filename))
drawMat = meta.mat;
drawMatDes = meta.matDes;

[Y,I] = sort(drawMatDes(1,:),'descend');
g1 = I(find(Y>1));
g2t = I(find(Y<1));
g3t = I(find(Y<0.5));
g3t2 = I(find(Y<0.2));
g2 = setdiff(g2t,g3t);
g3 = setdiff(g3t,g3t2);
% plot(1:length(Y),Y)

%% group 1
close all;
f1 = figure;
f1.Color = 'w';
f1.Position = [1066 155 790 879];

c = jet((length(g1)));
for i = 1:length(g1)
    p = plot(yearRange,drawMatDes(:,g1(i)),'-','linewidth',0.7,'Color',c(i,:));hold on;
    %     p2 = scatter(yearRange,drawMatDes(:,g1(i)),30,'filled','MarkerFaceAlpha',0.3,'MarkerFaceColor',c(i,:));
    p.Color(4) = 0.5;
    pairName = Label(idx2edge(g1(i),size(meta.mat,1)));
    legend([p],sprintf('%s-%s',pairName{1},pairName{2}))
end
legend('off')
legend('Location','bestoutside'	)
xticks(yearRange)
printname = sprintf('./figure/%s_temporal_g1',filename);
print(f1,'-dpng',printname)

%% group 2

close all;
f1 = figure;
f1.Color = 'w';
f1.Position = [1066 155 790 879];

c = jet((length(g2)));
for i = 1:length(g2)
    p = plot(yearRange,drawMatDes(:,g2(i)),'-','linewidth',0.7,'Color',c(i,:));hold on;
    %     p2 = scatter(yearRange,drawMatDes(:,g1(i)),30,'filled','MarkerFaceAlpha',0.3,'MarkerFaceColor',c(i,:));
    p.Color(4) = 0.5;
    pairName = Label(idx2edge(g2(i),size(meta.mat,1)));
    legend([p],sprintf('%s-%s',pairName{1},pairName{2}))
end
legend('off')
legend('Location','bestoutside'	)
xticks(yearRange)
printname = sprintf('./figure/%s_temporal_g2',filename);
print(f1,'-dpng',printname)


%% group 3

close all;
f1 = figure;
f1.Color = 'w';
f1.Position = [1066 155 790 879];

c = jet((length(g3)));
for i = 1:length(g3)
    p = plot(yearRange,drawMatDes(:,g3(i)),'-','linewidth',0.7,'Color',c(i,:));hold on;
    %     p2 = scatter(yearRange,drawMatDes(:,g1(i)),30,'filled','MarkerFaceAlpha',0.3,'MarkerFaceColor',c(i,:));
    p.Color(4) = 0.5;
    pairName = Label(idx2edge(g3(i),size(meta.mat,1)));
    legend([p],sprintf('%s-%s',pairName{1},pairName{2}))
end
legend('off')
legend('Location','bestoutside'	)
xticks(yearRange)
printname = sprintf('./figure/%s_temporal_g3',filename);
print(f1,'-dpng',printname)
