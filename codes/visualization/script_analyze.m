addpath(genpath('/home/heejong/HDD2T/matlab_lib'))
close all

%% Collision weather 
% colWeaLabel = {'borough','street name',...
%     'injured person #','killed person #','insured pedestrian #',...
%     'killed pedestrian #','injured cyclist #','killed cyclist #','injured motorist #','killed motorist #',...
%     'vehicle 1 factor','vehicle 2 factor','vehicle type 1','vehicle type 2',...
%     'zipcode','time (collision)','visb','date','day','month','year','time (weather)','spd','temp','prcp'}
% 
% year = '2012'
% fileName = sprintf('./collision_weather%s',year);
% colWea = readline(fileName,[1 -1]');
% colWea = cell2mat(arrayfun(@(x) str2num(cell2mat(x)), colWea(2:2:end), 'UniformOutput', false));
% colWea = colWea + colWea';

%% Complain weather

% str2mat(strsplit(comCol{1},','))

% comWeaLabel = {'agency','complain type', 'location type',...
%     'street','address type','city','facility type','status',...
%     'borough','channel type',...
%     'zipcode','time (complain)','visb','yymmdd','day','month','year',...
%     'time (weather)','spd','temp','prcp'}
% 
% year = '2018'
% fileName = sprintf('./complain_weather%s',year);
% comWea = readline(fileName,[1 -1]');
% comWea = cell2mat(arrayfun(@(x) str2num(cell2mat(x)), comWea(2:2:end), 'UniformOutput', false));
% comWea = comWea + comWea';

%% Complain collision
% comColLabel = {'agency','complain type', 'location type',...
%     'street (complain)','address type','city','facility type','status',...
%     'borough (complain)','channel type',...
%     'zipcode (complain)','time (complain)','borouth (collision)','street (collision)',...
%     'injured person #','killed person #','insured pedestrian #',...
%     'killed pedestrian #','injured cyclist #','killed cyclist #',...
%     'injured motorist #','killed motorist #',...
%     'vehicle 1 factor','vehicle 2 factor','vehicle type 1',...
%     'vehicle type 2',...
%     'yymmdd','day','month','year','zipcode (collision)','time'};
% 
% % %For 2012,2013
% % year = '2012'
% % fileName = sprintf('./complain_collision%s',year);
% % comCol = readline(fileName,[1 -1]');
% % comCol([1:2]) = [];
% % str2mat(strsplit(comCol{1},','))
% % comCol = cell2mat(arrayfun(@(x) str2num(cell2mat(x)), comCol(2:2:end), 'UniformOutput', false));
% % comCol(:,1) = [];
% % comCol = comCol + comCol';
% 
% % %For other years
% year = '2016'
% fileName = sprintf('./complain_collision%s',year);
% comCol = readline(fileName,[1 -1]');
% str2mat(strsplit(comCol{1},','))
% comCol = cell2mat(arrayfun(@(x) str2num(cell2mat(x)), comCol(2:2:end), 'UniformOutput', false));
% comCol = comCol + comCol';


%% Complain crime
% comCriLabel = {'agency',...
%     'complain type',...
%     'location type',...
%     'street (complain)','address type','city',...
%     'facility type','status',...
%     'borough (complain)','channel type',...
%     'zipcode (complain)','time (complain)',...
%     'offense code','offense code2','success code','offense level',...
%     'juridiction','borough (crime)','precinct','location desc',...
%     'premise','zipcode (crime)','yymmdd','day','month'...
%     'year','time'};
% 
% % %For other years
% year = '2016'
% fileName = sprintf('./complain_crime%s',year);
% comCri = readline(fileName,[1 -1]');
% str2mat(strsplit(comCri{1},','))
% comCri = cell2mat(arrayfun(@(x) str2num(cell2mat(x)), comCri(2:2:end), 'UniformOutput', false));
% comCri = comCri + comCri';


%% Weather Crime
% weaCriLabel = {'visb','time (weather)',...
%     'spd','temp','prcp',...
%     'offense code','offense code2','success code','offense level',...
%     'juridiction','borough (crime)','precinct','location desc',...
%     'premise','zipcode (crime)','yymmdd','day','month'...
%     'year','time'    };
% 
% % %For other years
% year = '2016'
% fileName = sprintf('./weather_crime%s',year);
% weaCri = readline(fileName,[1 -1]');
% str2mat(strsplit(weaCri{1},','))
% weaCri = cell2mat(arrayfun(@(x) str2num(cell2mat(x)), weaCri(2:2:end), 'UniformOutput', false));
% weaCri = weaCri + weaCri';

%% Weather Bike
weaBikLabel = {'visb','time (weather)',...
    'spd','temp','prcp',...
    'usertype','gender','start zipcode','end zipcode',...
    'trip time', 'yymmdd','day','month','year','time (bike)','age'};

% %For other years
year = '2013'
fileName = sprintf('./weather_bike%s',year);
weaBik = readline(fileName,[1 -1]');
str2mat(strsplit(weaBik{1},','))
weaBik = cell2mat(arrayfun(@(x) str2num(cell2mat(x)), weaBik(2:2:end), 'UniformOutput', false));
weaBik = weaBik + weaBik';

%% Find top N pairs 
Label = weaBikLabel;
data = weaBik;
% 
% topN = 10
% [B, I] = sort(dsqrform(sort(data)),'descend');
% Label(idx2edge(I(1:topN), size(data,1)))

%% Draw correlation matrix
f = figure;
f.Position = [2118 214 786 681];
f.Color = 'w';

% % Detail2
% printname = sprintf('./figure/%s_detail2',fileName);
% idx = 1:length(data);
% idx([20,19,18,13]) = [];
% redMap = cbrewer('seq', 'Oranges', 10);
% redMap = redMap(1:8,:)

% % Detail1
% printname = sprintf('./figure/%s_detail',fileName);
% idx = [1:length(data)-4,length(data)-2,length(data)];
% redMap = cbrewer('seq', 'Oranges', 10);

% % All 
printname = sprintf('./figure/%s',fileName);
idx = [1:size(data,1)];
redMap = cbrewer('seq', 'Reds', 10);

imagesc(data(idx,idx))

colormap(redMap);
colorbar

set(gca,'XTick', 1:size(data,2), 'XTickLabel', Label(idx))
set(gca,'YTick', 1:size(data,1), 'YTickLabel', Label(idx))
xtickangle(90)

print(f,'-dpng',printname)



