import * as Prando from 'prando';

const digits = [
  '',
  'one',
  'two',
  'three',
  'four',
  'five',
  'six',
  'seven',
  'eight',
  'nine'
];
const names100 = [
  ...digits,
  ...[
    'ten',
    'eleven',
    'twelve',
    'thirteen',
    'fourteen',
    'fifteen',
    'sixteen',
    'seventeen',
    'eighteen',
    'nineteen'
  ],
  ...digits.map((digit) => `twenty${digit != '' ? '-' + digit : ''}`),
  ...digits.map((digit) => `thirty${digit != '' ? '-' + digit : ''}`),
  ...digits.map((digit) => `forty${digit != '' ? '-' + digit : ''}`),
  ...digits.map((digit) => `fifty${digit != '' ? '-' + digit : ''}`),
  ...digits.map((digit) => `sixty${digit != '' ? '-' + digit : ''}`),
  ...digits.map((digit) => `seventy${digit != '' ? '-' + digit : ''}`),
  ...digits.map((digit) => `eighty${digit != '' ? '-' + digit : ''}`),
  ...digits.map((digit) => `ninety${digit != '' ? '-' + digit : ''}`)
];

type PrandoInstance = import('prando').default;
type PrandoConstructor = new (seed?: number | string) => PrandoInstance;

function resolvePrandoConstructor(): PrandoConstructor {
  const mod = Prando as unknown as {
    default?: PrandoConstructor;
  };
  if (typeof (Prando as unknown) === 'function') {
    return Prando as unknown as PrandoConstructor;
  }
  if (mod.default) {
    return mod.default;
  }
  throw new Error('Unable to locate Prando constructor');
}

const PRANDO_CTOR = resolvePrandoConstructor();

export function createRandom(seed?: number | string): PrandoInstance {
  return new PRANDO_CTOR(seed);
}

export function numberName(n: number): string {
  if (n == 0) {
    return 'zero';
  }

  let name: string[] = [];
  const d43 = Math.floor(n / 1000);
  if (d43 != 0) {
    name.push(names100[d43]);
    name.push('thousand');
    n -= d43 * 1000;
  }

  const d2 = Math.floor(n / 100);
  if (d2 != 0) {
    name.push(names100[d2]);
    name.push('hundred');
    n -= d2 * 100;
  }

  const d10 = n;
  if (d10 != 0) {
    name.push(names100[d10]);
  }

  return name.join(' ');
}
