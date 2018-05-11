function LINES = readline(FILE,NLINE,TYPE,varargin)
%READLINE   Reads specific lines from an ascii file.
%
% SYNTAX
%    LINES = readline(FILE);                    % Reads first line.
%    LINES = readline(FILE,NLINE);              % Reads specified lines.
%    LINES = readline(FILE,NLINE,TYPE);         % Specifies output type.
%    LINES = readline(FILE,NLINE,TYPE, ...,P/V); % Optional TEXTSCAN inputs.
%
% INPUTS
%     FILE - Name of the file to be read (string).
%    NLINE - Should be one of:
%               * A SINGLE INTEGER indicating the line number to be read
%                 from the beginning (if positive) or from the End-of-File
%                 (if negative).
%               * A ROW VECTOR of integers specifying the lines to be read
%                 (may have negative values).
%               * A TWO ROW MATRIX specifying ranges of lines to be read.
%                 See NOTE below for details. 
%               * 'all' reads the whole file (new since v4.0). Same as
%                 [1; -1]. Note, the output is always a cell of strings if
%                 not used 'string'.
%            DEFAULT: 1 (reads the first line of FILE)
%     TYPE - Specifies the output type. One of
%                 'cell' or FALSE - Forces cell output (TEXTSCAN default)
%               'string' or TRUE  - Forces string output with empty spaces
%                                   as padding. See NOTE below.
%            DEFAULT: 'string' if NLINE is single but 'cell' otherwise.
%      P/V - Pairwise optional inputs for the TEXTSCAN function, except
%            'delimiter' and 'whitespace'.
%            DEFAULT: none
%
% OUTPUTS
%    LINES - Read lines. BE CAREFUL: may be string or cell type.
%
% DESCRIPTION:
%    This function reads specified line(s) from an ascii file from the
%    beginning (positive NLINE) or the end of file (negatives NLINE) in a
%    very easy, fast and clean way.
%
%    Then, the user may get what he is looking for by searching through the
%    cell of string or the string matrix input (see the last example here
%    included).
%
% NOTES
%    * Optional inputs use its DEFAULT value when not given or [].
%    * If NLINE is empty, [], the program does not reads anything at all.
%    * Negative values in NLINE indicates line number from the End-of-File,
%      that is, -1 goes for the last line, -2 for the penultimate line, and
%      so on.
%    * The program differentiates between a row vector and a column vector
%      NLINE so the user be able to get ranges of lines with negative NLINE
%      elements and without carrying about the length of file. For 
%      example:
%         >> readline(FILE,[1 -1]')
%      reads the whole file, but
%         >> readline(FILE,[1 -1])
%      reads only the first and last lines (sometimes desirable), same as
%         >> readline(FILE,[-1:1])
%      But
%         >> readline(FILE,[1:-1])
%      do not reads anything at all because NLINE is empty.
%    * TYPE string inputs may be as short as a single char 's' or 'c'.
%    * If the file is too large, use the 'bufsize' optional argument (see
%      TEXTSCAN for details).
%
% EXAMPLE
%    FILE = 'readline.m';
%    % Reads from this file the:
%    readline(FILE)                       % First line
%    readline(FILE,71)                    % This line
%    readline(FILE,[67:71 73:76],'s')     % Examples except this one
%    readline(FILE,[1 4; -4 -1]','s')     % First and last 4 lines
%    readline(FILE,'all')                 % Whole file
%    L = readline(FILE,[-1 1]')           % Whole file backwards!
%    flipud(L(strncmp(strtrim(L),'%',1))) % Get all commented lines from L
%
% SEE ALSO
%    FGETL, TEXTSCAN, DLMREAD, LOAD
%    and
%    SAVEASCII by Carlos Vargas
%    at http://www.mathworks.com/matlabcentral/fileexchange
%
%
% --------
%    MFILE  readline.m
%  VERSION  4.0 (Jan 01, 2016) (<a href="matlab:web('http://www.mathworks.com/matlabcentral/fileexchange/?term=authorid%3A11258')">download</a>)
%   MATLAB  8.4.0.150421 (R2014b)
%   AUTHOR  Carlos Adrian Vargas Aguilera (MEXICO)
%  CONTACT  nubeobscura@hotmail.com

% REVISIONS
%  1.0      Released. (May 22, 2008).
%  2.0      Great improvement by using TEXTREAD instead of FGETL as
%           suggested by Urs (us) Schwarz. New easy way for the lines
%           inputs. Now accepts ascending and descending (negative) ranges.
%           Do not accept file identifier input anymore. (Jun 10, 2008)
%  3.0      Rewritten code. Added try catch error for string conversion. Do
%           not uses default 'bufsize' TEXTREAD option. Do not accept Inf
%           values in NLINE anymore. Negative NLINES values now indicated
%           number of line from the End-of-File, instead of from the last
%           line. Forces first 3 inputs positions. New pairwise optional
%           inputs for TEXTREAD. (Jun 08, 2009)
%  4.0      Returns an error if FILE was not found. Now uses TEXTSCAN
%           instead of TEXTREAD (Jan 01, 2016) 

% DISCLAIMER:
% READLINE.M is provided "as is" without warranty of any kind, under the
% revised BSD license.

% Copyright (c) 2008-2016 Carlos Adrian Vargas Aguilera


% PREAMBLE

% Checks number of inputs
assert(nargin>=1,'READLINE:NotEnoughInputs', ...
  'At least 1 input is required.')
assert(nargout<=1,'READLINE:TooManyOutputs', ...
  'At most 1 output is allowed.')

% Checks NLINE
if nargin<2
    NLINE = 1;        % Reads only the first line (default)
elseif ischar(NLINE)
    a = 'all';
    assert(isequal(lower(NLINE),a(1:min(length(NLINE),3))), ...
        'READLINE:IncorrectNlineAll', ...
        'NLINE must be numbers of lines or ''all''.')
    NLINE = [1; -1];  % Reads the whole file (v4.0)
end
assert(isnumeric(NLINE) && ismember(numel(NLINE),size(NLINE,2)*[1 2]) ...
    && all(isfinite(NLINE(:))), ...
    'READLINE:IncorrectNlineMatrix', ...
    'NLINE may have two rows at most.')
NLINE = round(NLINE); % forces integer values.

% Check TYPE
if nargin<3,
 TYPE = [];   % depends on NLINE
elseif ~isempty(TYPE)
 if isnumeric(TYPE), TYPE = logical(TYPE); end
 s = 'string';
 c = 'cell';
 switch lower(TYPE)
  case {s(1:min(length(TYPE),6)),true}
   % 'string'
   TYPE = true;
  case {c(1:min(length(TYPE),4)),false,0}
   % 'cell'
   TYPE = false;
  otherwise
   error('READLINE:IncorrectType', ...
    'TYPE must be one of ''string'' or ''cell''.')
 end
end

% Check varargin
nv = length(varargin);
c = 1;
while (c<=(nv-1))
 d = 'delimiter';
 w = 'whitespace';
 switch lower(varargin{c})
  case d(1:min(length(varargin{c}),9))
   varargin(c:c+1) = [];
   nv = nv-2;
   warning('READLINE:IgnoredDelimiterInput', ...
    'Ignored ''delimiter'' optional input.')
  case w(1:min(length(varargin{c}),10))
   varargin(c:c+1) = [];
   nv = nv-2;
   warning('READLINE:IgnoredWhitespaceInput', ...
    'Ignored ''whitespace'' optional input.')
  otherwise
   c = c+2;
 end
end


% MAIN

% Gets NLINE size
[m,n] = size(NLINE);

% Checks TYPE
if isempty(TYPE)
 if (m*n==1)
  TYPE = true;
 else
  TYPE = false;
 end
end

% Checks NLINE
if isempty(NLINE) || ((m*n==1) && (NLINE==0))
 % Finish if no lines will be read
 if ~TYPE
  LINES = {};
 else
  LINES = [];
 end
 return
end

% Gets the maximum line to be read
ineg = (NLINE<0);
maxl = max(NLINE(:));
if any(ineg(:))  % Forced to read the whole text
 maxl = -1;
end

% Checks FILE existance (v4.0)
if (exist(FILE,'file')~=2) && (exist(FILE,'builtin')~=5)
 error('READLINE:NotFoundFile', ...
  'Unable to find "%s" file.',FILE)
end

% Opens the file
fid = fopen(FILE,'rt');

% % Sets buffer size (v4.0)
% buffer = dir(FILE);
% buffer = buffer.bytes;

% Reads up to the maximum line in cell array via TEXTSCAN
LINES = textscan(fid,'%s',maxl, ...
 'delimiter' ,'\n'   , ...
 'whitespace',''     , ... % 'bufsize'   ,buffer , ...
 varargin{:});
if ~isempty(LINES)
    LINES = LINES{1};
end

% Close file
fclose(fid);

% % CODE using FGETL instead of TEXTSCAN
% %  Reads up to the maximum line in cell array via FGETL loop:
% if exist(FILE)~=2
%  error('Readline:IncorrectFileName','Not found such file.')
% end
% fid = fopen(FILE);
% if maxl>0
%  LINES = cell(maxl,1);
%  for k==1:maxl
%   LINES{k} = fgetl(fid);
%   if ~ischar(LINES{k})
%    LINES(k:maxl) = [];
%    break
%   end
%  end
% else
%  k = 1;
%  while 1
%   LINES{k} = fgetl(fid);
%   if ~ischar(LINES{k})
%    LINES(k) = [];
%    break
%   end
%   k = k+1;
%  end
% end
% fclose(fid);

% SELECTS SPECIFIED LINES
% Changes negative values
nlines      = length(LINES);
NLINE(ineg) = nlines + NLINE(ineg) + 1; % Changed from v3.0
% Checks if specified more lines than they exist
ip = (NLINE>nlines); % Lines beyond end of files!
in = (NLINE<1);      % Lines before first line!
% Deletes lines outside range
ibad          = all(ip,1)|all(in,1);
NLINE(:,ibad) = [];
ip(:,ibad)    = [];
in(:,ibad)    = [];
n             = size(NLINE,2);
if isempty(NLINE)
 % All lines outside
 if ~TYPE
  LINES = {};
 else
  LINES = [];
 end
 warning('READLINE:IgnoredNlineInput', ...
   'All specified lines lie outside file range.')
 return
end
if m==2
 % Truncates ranges to file range
 if any(ip(:)) || any(in(:))
  NLINE(ip) = nlines;
  NLINE(in) = 1;
  warning('READLINE:TruncatedNlineValues', ...
   'NLINE elements larger than file length were truncated.')
 end
 % Generates ranges
 nlinetemp = cell(1,n);
 for k = 1:n
  if NLINE(1,k)<=NLINE(2,k)
   nlinetemp{k} = NLINE(1,k):NLINE(2,k);
  else
   nlinetemp{k} = NLINE(1,k):-1:NLINE(2,k);
  end
 end
 NLINE = cell2mat(nlinetemp);
 if isempty(NLINE)
  if ~TYPE
   LINES = {};
  else
   LINES = [];
  end
  return
 end
end

%  Selects the specified lines
LINES = LINES(NLINE);


% FINISHING

%  Changes to string matrix
if TYPE
 try
	 LINES = char(LINES);
 catch
  warning('READLINE:CharConvertionError', ...
   'Output could not be transformed from cell to string. Maybe too large.')
 end
end

% [EOF] READLINE.M by nubeobscura@hotmail.com