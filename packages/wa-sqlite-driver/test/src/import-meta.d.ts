interface ImportMeta {
  glob(
    pattern: string,
    options: { as: 'raw'; eager: true }
  ): Record<string, string>;
}
